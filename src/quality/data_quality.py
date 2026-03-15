# =============================================================================
# src/quality/data_quality.py
#
# Pourquoi valider la qualite des donnees ?
#
#   Sans validation :
#     - Des NULL dans order_id -> les upserts Delta ecrasent des lignes au hasard
#     - Des doublons en Silver -> les aggregations Gold sont fausses
#     - Des status inconnus   -> les dashboards affichent des categories inconnues
#     - Le probleme est decouvert 3 jours plus tard par un analyste, pas par le job
#
#   Avec validation (avant l'ecriture) :
#     - Le job echoue immediatement avec un message clair
#     - Les donnees corrompues n'atteignent jamais Silver/Gold
#     - Les resultats sont stockes en Gold pour suivre la tendance de qualite
#
# Architecture du moteur DQ :
#
#   BaseCheck                    <- contrat abstrait
#       NotNullCheck             <- pas de NULL dans une colonne
#       UniquenessCheck          <- pas de doublons sur une/plusieurs colonnes
#       AcceptedValuesCheck      <- valeurs dans une liste autorisee
#       RowCountCheck            <- nombre de lignes dans un intervalle
#       RangeCheck               <- valeurs numeriques dans [min, max]
#
#   DataQualityChecker           <- orchestrateur : execute les checks, collecte les resultats
#   CheckResult                  <- resultat d'un check individuel (dataclass)
#   Severity                     <- WARN (log) ou FAIL (bloque le job)
#   DataQualityError             <- exception levee par assert_no_failures()
#
# Usage :
#   checker = (
#       DataQualityChecker("silver.orders")
#       .add_check(NotNullCheck("order_id"))
#       .add_check(UniquenessCheck("order_id"))
#       .add_check(AcceptedValuesCheck("status", ["PENDING", "SHIPPED"]))
#   )
#   results = checker.run(df)
#   checker.assert_no_failures()   # leve DataQualityError si FAIL non passe
# =============================================================================

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from src.utils.logger import get_logger

logger = get_logger(__name__)


# =============================================================================
# Severity — niveau de gravite d'un check
# =============================================================================


class Severity(str, Enum):
    """
    Niveau de gravite d'un check de qualite.

    WARN : le check echoue -> log un warning -> le pipeline continue
           Cas d'usage : valeurs inattendues non bloquantes, anomalies mineures

    FAIL : le check echoue -> log une erreur -> assert_no_failures() leve DataQualityError
           Cas d'usage : cles primaires nulles, doublons sur PK, types incorrects
    """

    WARN = "warn"
    FAIL = "fail"


# =============================================================================
# CheckResult — resultat d'un check individuel
# =============================================================================


@dataclass
class CheckResult:
    """
    Resultat complet d'un check de qualite.

    Stocke toutes les metriques necessaires pour le monitoring et le debug :
    nombre de lignes totales, nombre en echec, taux de reussite, details.

    Peut etre persiste en Delta Lake pour suivre la tendance de qualite.
    """

    check_name: str
    dataset: str
    status: str  # "PASS" | "FAIL" | "WARN"
    severity: Severity
    total_rows: int
    failing_rows: int
    passing_rows: int
    pass_rate: float
    details: dict[str, Any] = field(default_factory=dict)
    run_ts: str = field(default_factory=lambda: datetime.now(tz=timezone.utc).isoformat())

    @property
    def passed(self) -> bool:
        """True si le check a passe (status == PASS)."""
        return self.status == "PASS"

    def to_dict(self) -> dict[str, Any]:
        """Serialise en dict pour le logging et la persistance Delta."""
        return {
            "check_name": self.check_name,
            "dataset": self.dataset,
            "status": self.status,
            "severity": self.severity.value,
            "total_rows": self.total_rows,
            "failing_rows": self.failing_rows,
            "passing_rows": self.passing_rows,
            "pass_rate": round(self.pass_rate, 6),
            "details": json.dumps(self.details),
            "run_ts": self.run_ts,
        }

    def __str__(self) -> str:
        return (
            f"[{self.status}] {self.check_name} | "
            f"pass_rate={self.pass_rate:.1%} | "
            f"failing={self.failing_rows}/{self.total_rows}"
        )


# =============================================================================
# BaseCheck — contrat abstrait pour tous les checks
# =============================================================================


class BaseCheck:
    """
    Contrat abstrait pour un check de qualite.

    Chaque sous-classe implemente run(df, dataset) -> CheckResult.
    Le check ne leve jamais d'exception — il retourne toujours un CheckResult.
    C'est DataQualityChecker.assert_no_failures() qui decide de bloquer ou non.
    """

    def __init__(self, name: str, severity: Severity = Severity.FAIL) -> None:
        self.name = name
        self.severity = severity

    def run(self, df: DataFrame, dataset: str) -> CheckResult:
        raise NotImplementedError(f"{self.__class__.__name__} doit implementer run()")


# =============================================================================
# NotNullCheck
# =============================================================================


class NotNullCheck(BaseCheck):
    """
    Verifie qu'une colonne ne contient pas de valeurs NULL.

    Un taux de NULL superieur au threshold entraine un echec.

    Args:
        column    : Colonne a verifier.
        severity  : FAIL (defaut) ou WARN.
        threshold : Taux de NULL accepte (0.0 = aucun NULL, 0.05 = 5% max).

    Example:
        NotNullCheck("order_id")                    # aucun NULL tolere
        NotNullCheck("email", severity=Severity.WARN)
        NotNullCheck("city", threshold=0.10)        # jusqu'a 10% de NULL OK
    """

    def __init__(
        self,
        column: str,
        severity: Severity = Severity.FAIL,
        threshold: float = 0.0,
    ) -> None:
        super().__init__(f"not_null:{column}", severity)
        self.column = column
        self.threshold = threshold

    def run(self, df: DataFrame, dataset: str) -> CheckResult:
        total = df.count()
        failing = df.filter(F.col(self.column).isNull()).count()
        null_rate = failing / total if total > 0 else 0.0
        passed = null_rate <= self.threshold
        return CheckResult(
            check_name=self.name,
            dataset=dataset,
            status="PASS" if passed else self.severity.value.upper(),
            severity=self.severity,
            total_rows=total,
            failing_rows=failing,
            passing_rows=total - failing,
            pass_rate=1.0 - null_rate,
            details={
                "column": self.column,
                "null_rate": round(null_rate, 6),
                "threshold": self.threshold,
            },
        )


# =============================================================================
# UniquenessCheck
# =============================================================================


class UniquenessCheck(BaseCheck):
    """
    Verifie qu'il n'y a pas de valeurs dupliquees sur une ou plusieurs colonnes.

    Args:
        columns   : Colonne(s) formant la cle de deduplication.
        severity  : FAIL (defaut) ou WARN.
        threshold : Taux de doublons accepte (0.0 = aucun doublon).

    Example:
        UniquenessCheck("order_id")
        UniquenessCheck(["order_id", "line_id"])   # cle composite
        UniquenessCheck("email", threshold=0.001)  # 0.1% de doublons OK
    """

    def __init__(
        self,
        columns: str | list[str],
        severity: Severity = Severity.FAIL,
        threshold: float = 0.0,
    ) -> None:
        cols = [columns] if isinstance(columns, str) else columns
        super().__init__(f"unique:{','.join(cols)}", severity)
        self.columns = cols
        self.threshold = threshold

    def run(self, df: DataFrame, dataset: str) -> CheckResult:
        total = df.count()
        distinct = df.select(*self.columns).distinct().count()
        dup_count = total - distinct
        dup_rate = dup_count / total if total > 0 else 0.0
        passed = dup_rate <= self.threshold
        return CheckResult(
            check_name=self.name,
            dataset=dataset,
            status="PASS" if passed else self.severity.value.upper(),
            severity=self.severity,
            total_rows=total,
            failing_rows=dup_count,
            passing_rows=distinct,
            pass_rate=1.0 - dup_rate,
            details={
                "columns": self.columns,
                "duplicates": dup_count,
                "dup_rate": round(dup_rate, 6),
                "threshold": self.threshold,
            },
        )


# =============================================================================
# AcceptedValuesCheck
# =============================================================================


class AcceptedValuesCheck(BaseCheck):
    """
    Verifie que toutes les valeurs d'une colonne sont dans une liste autorisee.

    Par defaut WARN (pas FAIL) : les valeurs inconnues sont souvent des
    donnees source que l'on ne controle pas totalement.

    Args:
        column          : Colonne a verifier.
        accepted_values : Liste des valeurs autorisees.
        severity        : WARN (defaut) ou FAIL.

    Example:
        AcceptedValuesCheck("status", ["PENDING", "SHIPPED", "DELIVERED"])
        AcceptedValuesCheck("currency", ["USD", "EUR"], severity=Severity.FAIL)
    """

    def __init__(
        self,
        column: str,
        accepted_values: list[Any],
        severity: Severity = Severity.WARN,
    ) -> None:
        super().__init__(f"accepted_values:{column}", severity)
        self.column = column
        self.accepted_values = accepted_values

    def run(self, df: DataFrame, dataset: str) -> CheckResult:
        total = df.count()
        failing = df.filter(~F.col(self.column).isin(self.accepted_values)).count()
        pass_rate = 1.0 - (failing / total if total > 0 else 0.0)
        return CheckResult(
            check_name=self.name,
            dataset=dataset,
            status="PASS" if failing == 0 else self.severity.value.upper(),
            severity=self.severity,
            total_rows=total,
            failing_rows=failing,
            passing_rows=total - failing,
            pass_rate=pass_rate,
            details={
                "column": self.column,
                "accepted_values": self.accepted_values,
            },
        )


# =============================================================================
# RowCountCheck
# =============================================================================


class RowCountCheck(BaseCheck):
    """
    Verifie que le nombre de lignes est dans un intervalle attendu.

    Utile pour detecter : tables vides, truncation inattendue, explosion de donnees.

    Args:
        min_rows : Nombre minimum de lignes (defaut: 1 = table non vide).
        max_rows : Nombre maximum de lignes (None = pas de limite).
        severity : WARN (defaut) ou FAIL.

    Example:
        RowCountCheck(min_rows=1)                       # table non vide
        RowCountCheck(min_rows=1000, max_rows=1_000_000)
    """

    def __init__(
        self,
        min_rows: int = 1,
        max_rows: int | None = None,
        severity: Severity = Severity.WARN,
    ) -> None:
        super().__init__("row_count", severity)
        self.min_rows = min_rows
        self.max_rows = max_rows

    def run(self, df: DataFrame, dataset: str) -> CheckResult:
        total = df.count()
        passed = total >= self.min_rows and (self.max_rows is None or total <= self.max_rows)
        return CheckResult(
            check_name=self.name,
            dataset=dataset,
            status="PASS" if passed else self.severity.value.upper(),
            severity=self.severity,
            total_rows=total,
            failing_rows=0 if passed else 1,
            passing_rows=total if passed else 0,
            pass_rate=1.0 if passed else 0.0,
            details={
                "actual": total,
                "min_rows": self.min_rows,
                "max_rows": self.max_rows,
            },
        )


# =============================================================================
# RangeCheck
# =============================================================================


class RangeCheck(BaseCheck):
    """
    Verifie que les valeurs d'une colonne numerique sont dans [min_val, max_val].

    Args:
        column  : Colonne numerique a verifier.
        min_val : Valeur minimale autorisee (None = pas de borne inferieure).
        max_val : Valeur maximale autorisee (None = pas de borne superieure).
        severity: FAIL (defaut) ou WARN.

    Example:
        RangeCheck("unit_price", min_val=0.0)           # prix positif
        RangeCheck("quantity",   min_val=1, max_val=9999)
        RangeCheck("discount",   min_val=0.0, max_val=1.0)
    """

    def __init__(
        self,
        column: str,
        min_val: float | None = None,
        max_val: float | None = None,
        severity: Severity = Severity.FAIL,
    ) -> None:
        super().__init__(f"range:{column}", severity)
        self.column = column
        self.min_val = min_val
        self.max_val = max_val

    def run(self, df: DataFrame, dataset: str) -> CheckResult:
        total = df.count()
        condition = F.lit(False)
        if self.min_val is not None:
            condition = condition | (F.col(self.column) < self.min_val)
        if self.max_val is not None:
            condition = condition | (F.col(self.column) > self.max_val)
        failing = df.filter(condition).count()
        pass_rate = 1.0 - (failing / total if total > 0 else 0.0)
        return CheckResult(
            check_name=self.name,
            dataset=dataset,
            status="PASS" if failing == 0 else self.severity.value.upper(),
            severity=self.severity,
            total_rows=total,
            failing_rows=failing,
            passing_rows=total - failing,
            pass_rate=pass_rate,
            details={
                "column": self.column,
                "min_val": self.min_val,
                "max_val": self.max_val,
            },
        )


# =============================================================================
# DataQualityChecker — orchestrateur
# =============================================================================


class DataQualityChecker:
    """
    Orchestrateur qui execute un ensemble de checks et collecte les resultats.

    Pattern fluide : chaque add_check() retourne self -> chainable.

    Example:
        checker = (
            DataQualityChecker("silver.orders")
            .add_check(RowCountCheck(min_rows=1))
            .add_check(NotNullCheck("order_id"))
            .add_check(UniquenessCheck("order_id"))
            .add_check(RangeCheck("unit_price", min_val=0.0))
            .add_check(AcceptedValuesCheck("status", VALID_STATUSES))
        )
        results = checker.run(df)
        checker.assert_no_failures()
    """

    def __init__(self, dataset_name: str) -> None:
        self.dataset_name = dataset_name
        self._checks: list[BaseCheck] = []
        self._results: list[CheckResult] = []

    def add_check(self, check: BaseCheck) -> DataQualityChecker:
        """Ajoute un check et retourne self pour le chainage."""
        self._checks.append(check)
        return self

    def run(self, df: DataFrame) -> list[CheckResult]:
        """
        Execute tous les checks enregistres et retourne les resultats.

        Chaque check est logue au niveau INFO (PASS) ou WARNING (FAIL/WARN).
        """
        self._results = []
        for check in self._checks:
            result = check.run(df, self.dataset_name)
            self._results.append(result)

            if result.passed:
                logger.info(
                    "DQ check",
                    extra={
                        "check": result.check_name,
                        "status": result.status,
                        "dataset": result.dataset,
                    },
                )
            else:
                logger.warning(
                    "DQ check",
                    extra={
                        "check": result.check_name,
                        "status": result.status,
                        "failing_rows": result.failing_rows,
                        "pass_rate": f"{result.pass_rate:.1%}",
                        "dataset": result.dataset,
                    },
                )

        return self._results

    def persist_results(self, spark: SparkSession, output_path: str) -> None:
        """
        Ecrit les resultats des checks dans une table Delta (mode append).

        Permet de suivre la tendance de qualite dans le temps et de
        construire des dashboards de monitoring qualite.

        Args:
            spark       : SparkSession active.
            output_path : Chemin Delta pour les resultats (ex: gold/dq_results).
        """
        if not self._results:
            logger.warning("Aucun resultat DQ a persister — appeler run() d'abord")
            return

        rows = [r.to_dict() for r in self._results]
        result_df = spark.createDataFrame(rows)
        result_df.write.mode("append").parquet(output_path)
        logger.info("Resultats DQ persistes", extra={"path": output_path, "checks": len(rows)})

    def assert_no_failures(self) -> None:
        """
        Leve DataQualityError si au moins un check de severite FAIL a echoue.

        Les checks WARN qui echouent sont ignores ici (loggues mais non bloquants).

        Raises:
            DataQualityError : avec la liste de tous les checks en echec.
        """
        failures = [r for r in self._results if not r.passed and r.severity == Severity.FAIL]
        if failures:
            summary = " | ".join(
                f"{r.check_name} ({r.failing_rows}/{r.total_rows} lignes)" for r in failures
            )
            raise DataQualityError(f"Echecs qualite sur '{self.dataset_name}' : {summary}")

    @property
    def summary(self) -> dict[str, Any]:
        """Synthese des resultats : total, passed, failed, pass_rate."""
        total = len(self._results)
        passed = sum(1 for r in self._results if r.passed)
        return {
            "dataset": self.dataset_name,
            "total_checks": total,
            "passed": passed,
            "failed": total - passed,
            "pass_rate": passed / total if total else 0.0,
        }


# =============================================================================
# DataQualityError
# =============================================================================


class DataQualityError(Exception):
    """
    Levee quand au moins un check de severite FAIL n'a pas passe.

    Le message contient le nom du dataset et la liste des checks en echec
    avec le nombre de lignes concernees.
    """
