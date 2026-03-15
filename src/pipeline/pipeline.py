# =============================================================================
# src/pipeline/pipeline.py
#
# Orchestration de pipeline — version simplifiee d'un DAG Airflow/ADF.
#
# Probleme sans orchestration :
#   Les jobs sont lances a la main, un par un.
#   Si un job echoue, les suivants tournent quand meme -> donnees incoherentes.
#   Pas de visibilite sur l'etat du pipeline dans son ensemble.
#
# Solution : Pipeline + PipelineStep
#   Chaque step declare ses dependances.
#   Si une dependance echoue -> tous les steps dependants passent en SKIPPED.
#   Un rapport final resume l'etat de chaque step.
#
# Analogie Airflow :
#   Pipeline     = DAG (Directed Acyclic Graph)
#   PipelineStep = Task / Operator
#   dependencies = upstream_tasks
#   StepStatus   = TaskInstance state (running, success, failed, skipped)
#
# Usage :
#   pipeline = (
#       Pipeline("orders_daily")
#       .add_step(PipelineStep("bronze_ingest",  BronzeIngestJob))
#       .add_step(PipelineStep("silver_orders",  OrdersEtlJob,
#                              dependencies=["bronze_ingest"]))
#       .add_step(PipelineStep("gold_kpis",      GoldKpisJob,
#                              dependencies=["silver_orders"]))
#   )
#   report = pipeline.run(spark)
#   report.print_summary()
# =============================================================================

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Type

from pyspark.sql import SparkSession

from jobs.base_job import BaseJob, JobMetrics
from utils.config import AppConfig
from utils.logger import get_logger

logger = get_logger(__name__)


# =============================================================================
# StepStatus — cycle de vie d'un step
# =============================================================================


class StepStatus(Enum):
    """
    Etats possibles d'un step de pipeline.

    PENDING  : en attente d'execution (dependances pas encore terminees)
    RUNNING  : en cours d'execution
    SUCCESS  : termine avec succes
    FAILED   : a leve une exception
    SKIPPED  : une dependance a echoue -> ce step est court-circuite

    En Airflow : none / running / success / failed / upstream_failed
    """

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


# =============================================================================
# PipelineStep — definition d'un step
# =============================================================================


@dataclass
class PipelineStep:
    """
    Definit un step dans le pipeline.

    Args:
        name         : Identifiant unique du step (ex: "silver_orders").
        job_class    : Classe du job a instancier (sous-classe de BaseJob).
        dependencies : Noms des steps qui doivent reussir avant ce step.
        config       : Config specifique. Si None, utilise AppConfig.from_env().
        retry_on_fail: Si True, rejoue le step une fois en cas d'echec.

    Example:
        PipelineStep(
            name="silver_orders",
            job_class=OrdersEtlJob,
            dependencies=["bronze_ingest"],
        )
    """

    name: str
    job_class: Type[BaseJob]
    dependencies: list[str] = field(default_factory=list)
    config: AppConfig | None = None
    retry_on_fail: bool = False


# =============================================================================
# StepResult — resultat d'execution d'un step
# =============================================================================


@dataclass
class StepResult:
    """
    Resultat d'execution d'un step.

    Attributes:
        step_name       : Nom du step.
        status          : SUCCESS / FAILED / SKIPPED.
        metrics         : Metriques du job (lignes lues/ecrites, DQ checks).
        error           : Message d'erreur si FAILED.
        elapsed_seconds : Duree d'execution du step (overhead pipeline inclus).
    """

    step_name: str
    status: StepStatus
    metrics: JobMetrics | None = None
    error: str | None = None
    elapsed_seconds: float = 0.0


# =============================================================================
# PipelineReport — rapport d'execution du pipeline complet
# =============================================================================


class PipelineReport:
    """
    Agregation des resultats de tous les steps.

    Fournit :
      - resume (passed / failed / skipped)
      - duree totale
      - affichage console lisible
    """

    def __init__(self, pipeline_name: str, results: list[StepResult]) -> None:
        self.pipeline_name = pipeline_name
        self.results = results
        self.total_elapsed = sum(r.elapsed_seconds for r in results)
        self.steps_success = [r for r in results if r.status == StepStatus.SUCCESS]
        self.steps_failed = [r for r in results if r.status == StepStatus.FAILED]
        self.steps_skipped = [r for r in results if r.status == StepStatus.SKIPPED]

    @property
    def success(self) -> bool:
        """True si aucun step n'a echoue."""
        return len(self.steps_failed) == 0

    def print_summary(self) -> None:
        """Affiche le rapport d'execution dans la console."""
        sep = "=" * 62
        print(f"\n{sep}")
        print(f"  PIPELINE : {self.pipeline_name}")
        print(f"  STATUT   : {'OK' if self.success else 'ECHEC'}")
        print(f"  DUREE    : {self.total_elapsed:.1f}s")
        print(sep)
        print(f"  {'STEP':<28}  {'STATUT':<8}  {'LIGNES':>7}  {'DQ':>6}")
        print(f"  {'-'*28}  {'-'*8}  {'-'*7}  {'-'*6}")

        for r in self.results:
            rows = "-"
            dq = "-"
            if r.metrics:
                rows = str(r.metrics.rows_written)
                total = r.metrics.dq_checks_passed + r.metrics.dq_checks_failed
                dq = f"{r.metrics.dq_checks_passed}/{total}"

            status_display = r.status.value
            print(f"  {r.step_name:<28}  {status_display:<8}  {rows:>7}  {dq:>6}")

            if r.status == StepStatus.FAILED and r.error:
                # Affiche la premiere ligne du message d'erreur
                first_line = r.error.split("\n")[0][:55]
                print(f"  {'':28}  -> {first_line}")

        print(sep)
        print(
            f"  {len(self.steps_success)} reussi(s)  "
            f"{len(self.steps_failed)} echoue(s)  "
            f"{len(self.steps_skipped)} passe(s)"
        )
        print(f"{sep}\n")


# =============================================================================
# Pipeline — orchestrateur principal
# =============================================================================


class Pipeline:
    """
    Orchestre une sequence de jobs avec gestion des dependances.

    Principe de base du DAG :
      - Les steps sont executes dans l'ordre d'ajout.
      - Un step n'est execute que si TOUTES ses dependances ont le statut SUCCESS.
      - Si une dependance est FAILED ou SKIPPED, le step passe en SKIPPED.
      - Le premier echec arrête l'execution (fail-fast).

    Idempotence :
      Chaque job utilisant Delta MERGE, relancer le pipeline produit
      le meme resultat sans doublons.

    Args:
        name  : Nom du pipeline (ex: "orders_daily_v1").
        spark : SparkSession injectee. Si None, creee a la premiere utilisation.

    Example:
        pipeline = (
            Pipeline("orders_daily")
            .add_step(PipelineStep("ingest",  IngestJob))
            .add_step(PipelineStep("silver",  OrdersEtlJob, ["ingest"]))
            .add_step(PipelineStep("gold",    GoldKpisJob,  ["silver"]))
        )
        report = pipeline.run(spark)
        report.print_summary()
    """

    def __init__(self, name: str, spark: SparkSession | None = None) -> None:
        self.name = name
        self._spark = spark
        self._steps: list[PipelineStep] = []

    def add_step(self, step: PipelineStep) -> Pipeline:
        """
        Ajoute un step au pipeline.
        Retourne self pour permettre le chaining fluent.
        """
        self._steps.append(step)
        return self

    def run(self, spark: SparkSession | None = None) -> PipelineReport:
        """
        Execute tous les steps dans l'ordre, en respectant les dependances.

        Args:
            spark : SparkSession a injecter dans chaque job.
                    Priorite : argument > constructeur > auto-creation.

        Returns:
            PipelineReport avec les resultats de tous les steps.
        """
        effective_spark = spark or self._spark

        logger.info("Pipeline demarre", extra={"pipeline": self.name, "steps": len(self._steps)})

        results: dict[str, StepResult] = {}

        for step in self._steps:
            result = self._run_step(step, results, effective_spark)
            results[step.name] = result

            # Fail-fast : si un step critique echoue, on arrete
            if result.status == StepStatus.FAILED:
                logger.error(
                    "Pipeline interrompu apres echec",
                    extra={"pipeline": self.name, "failed_step": step.name},
                )
                # Marquer les steps restants comme SKIPPED
                for remaining in self._steps:
                    if remaining.name not in results:
                        results[remaining.name] = StepResult(
                            step_name=remaining.name,
                            status=StepStatus.SKIPPED,
                            error="Step precedent en echec",
                        )
                break

        report = PipelineReport(self.name, list(results.values()))

        logger.info(
            "Pipeline termine",
            extra={
                "pipeline": self.name,
                "success": report.success,
                "elapsed": report.total_elapsed,
                "passed": len(report.steps_success),
                "failed": len(report.steps_failed),
                "skipped": len(report.steps_skipped),
            },
        )

        return report

    def _run_step(
        self,
        step: PipelineStep,
        results: dict[str, StepResult],
        spark: SparkSession | None,
    ) -> StepResult:
        """Execute un step en verifiant ses dependances."""

        # Verifier les dependances
        for dep_name in step.dependencies:
            dep_result = results.get(dep_name)
            if dep_result is None or dep_result.status != StepStatus.SUCCESS:
                logger.warning(
                    "Step saute : dependance non satisfaite",
                    extra={"step": step.name, "dependency": dep_name},
                )
                return StepResult(
                    step_name=step.name,
                    status=StepStatus.SKIPPED,
                    error=f"Dependance '{dep_name}' non satisfaite",
                )

        logger.info("Step demarre", extra={"step": step.name})
        start = time.monotonic()

        attempt = 0
        max_attempts = 2 if step.retry_on_fail else 1

        while attempt < max_attempts:
            attempt += 1
            try:
                # Instancier et executer le job
                job = step.job_class(config=step.config, spark=spark)
                metrics = job.execute()
                elapsed = time.monotonic() - start

                logger.info(
                    "Step termine",
                    extra={
                        "step": step.name,
                        "elapsed": round(elapsed, 3),
                        "rows": metrics.rows_written,
                    },
                )
                return StepResult(
                    step_name=step.name,
                    status=StepStatus.SUCCESS,
                    metrics=metrics,
                    elapsed_seconds=elapsed,
                )

            except Exception as exc:
                elapsed = time.monotonic() - start
                if attempt < max_attempts:
                    logger.warning(
                        "Step echoue, nouvelle tentative",
                        extra={"step": step.name, "attempt": attempt, "error": str(exc)},
                    )
                else:
                    logger.error(
                        "Step echoue definitivement", extra={"step": step.name, "error": str(exc)}
                    )
                    return StepResult(
                        step_name=step.name,
                        status=StepStatus.FAILED,
                        error=str(exc),
                        elapsed_seconds=elapsed,
                    )

        # Ne devrait jamais etre atteint
        return StepResult(step_name=step.name, status=StepStatus.FAILED)
