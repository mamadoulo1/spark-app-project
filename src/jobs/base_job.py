# =============================================================================
# src/jobs/base_job.py
#
# Pourquoi une classe de base pour les jobs ?
#
#   SANS BaseJob — chaque job duplique ce code :
#
#     def run_orders_job():
#         spark = SparkSession.builder...getOrCreate()
#         logger.info("Job demarre")
#         start = time.monotonic()
#         try:
#             # logique metier
#             ...
#         except Exception as e:
#             logger.error("Job echoue", extra={"error": str(e)})
#             raise
#         finally:
#             spark.stop()
#         logger.info("Job termine", extra={"elapsed": time.monotonic() - start})
#
#   Problemes :
#     - Ce bloc est copie-colle dans CHAQUE job (10 jobs = 10 copies)
#     - Un bug dans la gestion d'erreurs doit etre corrige partout
#     - Pas de standard pour les metriques ou les alertes
#
#   AVEC BaseJob — Template Method Pattern :
#     - Le "squelette" de l'algorithme (demarrage, metriques, arret, erreurs)
#       est ecrit une seule fois dans BaseJob.execute()
#     - Chaque job ne fait qu'implementer run() avec sa logique metier
#
# Pattern utilise : Template Method
#   BaseJob.execute() = algorithme fixe (ne pas surcharger)
#   run()    = methode abstraite (OBLIGATOIRE)
#   pre_run()    / post_run()   = hooks optionnels
#   on_failure() = hook optionnel pour les alertes
# =============================================================================

from __future__ import annotations

import time
import traceback
from abc import ABC, abstractmethod
from typing import Any

from pyspark.sql import SparkSession

from utils.config import AppConfig
from utils.logger import get_logger
from utils.spark_utils import get_spark_session, stop_spark_session

logger = get_logger(__name__)


# =============================================================================
# JobMetrics — accumulateur de metriques d'execution
# =============================================================================


class JobMetrics:
    """
    Accumule les metriques de runtime pour une execution de job.

    Un data engineer professionnel mesure toujours :
      - Combien de lignes ont ete lues / ecrites / rejetees
      - Combien de temps a pris le job
      - Est-ce que la qualite des donnees est bonne

    Ces metriques sont loggees en fin de job et peuvent etre envoyees
    vers un systeme de monitoring (Datadog, Prometheus, CloudWatch).
    """

    def __init__(self) -> None:
        self.start_time: float = 0.0
        self.end_time: float = 0.0
        self.rows_read: int = 0
        self.rows_written: int = 0
        self.rows_dropped: int = 0
        self.dq_checks_passed: int = 0
        self.dq_checks_failed: int = 0
        self.extra: dict[str, Any] = {}  # metriques personnalisees

    @property
    def elapsed_seconds(self) -> float:
        """Duree d'execution en secondes (arrondie a 3 decimales)."""
        return round(self.end_time - self.start_time, 3)

    def to_dict(self) -> dict[str, Any]:
        """Serialise les metriques en dict pour le logging et le monitoring."""
        return {
            "elapsed_seconds": self.elapsed_seconds,
            "rows_read": self.rows_read,
            "rows_written": self.rows_written,
            "rows_dropped": self.rows_dropped,
            "dq_checks_passed": self.dq_checks_passed,
            "dq_checks_failed": self.dq_checks_failed,
            **self.extra,
        }


# =============================================================================
# BaseJob — classe abstraite, parent de tous les jobs
# =============================================================================


class BaseJob(ABC):
    """
    Classe abstraite qui definit le cycle de vie de tous les jobs Spark.

    Les sous-classes DOIVENT implementer :
        run(self) -> None

    Les sous-classes PEUVENT surcharger :
        pre_run(self)           -> None   (setup avant run)
        post_run(self)          -> None   (nettoyage apres run reussi)
        on_failure(exc)         -> None   (alerte en cas d'erreur)

    Exemple minimal :
        class MonJob(BaseJob):
            JOB_NAME = "mon_job"

            def run(self) -> None:
                df = self.spark.read.parquet(self.config.get_zone_path("bronze", "orders"))
                df.write.parquet(self.config.get_zone_path("silver", "orders"))

    Utilisation :
        job = MonJob()
        metrics = job.execute()
        print(metrics.to_dict())
    """

    # Identifiant du job : apparait dans tous les logs et metriques.
    # Chaque sous-classe DOIT le redefinir.
    JOB_NAME: str = "base_job"

    def __init__(
        self,
        config: AppConfig | None = None,
        spark: SparkSession | None = None,
    ) -> None:
        """
        Args:
            config : Configuration de l'application. Si None, chargee depuis $ENV.
            spark  : SparkSession existante. Si None, une nouvelle est creee.
                     IMPORTANT : si vous injectez une session (cas des tests),
                     elle ne sera PAS arretee par execute(). C'est votre
                     responsabilite de l'arreter.
        """
        self.config = config or AppConfig.from_env()
        self._spark = spark
        self.metrics = JobMetrics()
        self._owns_spark = spark is None  # True = on a cree la session, on doit l'arreter

    @property
    def spark(self) -> SparkSession:
        """
        Acces lazy a la SparkSession.

        La session n'est creee qu'a la premiere utilisation de self.spark.
        Si une session a ete injectee au constructeur, elle est reutilisee.
        """
        if self._spark is None:
            self._spark = get_spark_session(self.config)
        return self._spark

    # ------------------------------------------------------------------
    # Methodes abstraites et hooks — a implementer ou surcharger
    # ------------------------------------------------------------------

    @abstractmethod
    def run(self) -> None:
        """
        Logique metier du job.

        C'est la SEULE methode obligatoire dans chaque sous-classe.
        Acces a Spark via self.spark, a la config via self.config,
        aux metriques via self.metrics.
        """

    def pre_run(self) -> None:
        """
        Hook execute AVANT run().
        Override pour : valider des paths, initialiser des connexions,
        verifier des pre-conditions.
        """

    def post_run(self) -> None:
        """
        Hook execute APRES un run() reussi.
        Override pour : notifier Slack, archiver des fichiers,
        declencher un job suivant.
        """

    def on_failure(self, exc: Exception) -> None:
        """
        Hook execute quand run() leve une exception.

        Par defaut : log l'erreur avec la stack trace.
        Override pour ajouter : alertes Slack, PagerDuty, emails, etc.

        IMPORTANT : toujours appeler super().on_failure(exc) pour garder le log.
        """
        logger.error(
            "Job echoue",
            extra={
                "job": self.JOB_NAME,
                "error": str(exc),
                "traceback": traceback.format_exc(),
            },
        )

    # ------------------------------------------------------------------
    # execute() — le coeur du pattern Template Method
    # Ne pas surcharger cette methode dans les sous-classes.
    # ------------------------------------------------------------------

    def execute(self) -> JobMetrics:
        """
        Orchestre le cycle de vie complet du job.

        Sequence :
          pre_run()
              ↓
          run()          ← logique metier (implementee par chaque job)
              ↓
          post_run()     ← seulement si run() reussit
          on_failure()   ← seulement si run() leve une exception
              ↓
          stop_spark()   ← toujours execute (bloc finally)

        Returns:
            JobMetrics avec timing et comptages de lignes.
        """
        logger.info("Job demarre", extra={"job": self.JOB_NAME, "env": self.config.env})
        self.metrics.start_time = time.monotonic()

        try:
            self.pre_run()
            self.run()
            self.metrics.end_time = time.monotonic()
            self.post_run()

            logger.info(
                "Job termine avec succes",
                extra={"job": self.JOB_NAME, "metrics": self.metrics.to_dict()},
            )

        except Exception as exc:
            self.metrics.end_time = time.monotonic()
            self.on_failure(exc)
            raise  # on re-leve l'exception pour que l'orchestrateur la voie

        finally:
            # Arreter la session UNIQUEMENT si on l'a creee nous-memes
            # (pas si elle a ete injectee depuis l'exterieur)
            if self._owns_spark and self._spark is not None:
                stop_spark_session(self._spark)

        return self.metrics
