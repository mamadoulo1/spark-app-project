# =============================================================================
# tests/unit/test_pipeline.py
#
# ETAPE 12 — Tests unitaires du module Pipeline (Step 11)
#
# Strategie de test :
#   Ces tests couvrent la LOGIQUE DAG du Pipeline sans SparkSession reelle.
#   On injecte un MagicMock comme SparkSession -> PySpark n'est pas demarre.
#   Les jobs de test (SuccessJob / FailingJob) n'accedent pas a self.spark,
#   donc aucune JVM n'est creee.
#
#   C'est exactement le principe des tests unitaires :
#     Tester la logique (dependances, fail-fast, retry)
#     SANS les effets de bord (Spark, Delta, reseau).
# =============================================================================

from __future__ import annotations

import pytest
from unittest.mock import MagicMock

from src.jobs.base_job import BaseJob
from src.pipeline.pipeline import (
    Pipeline,
    PipelineReport,
    PipelineStep,
    StepResult,
    StepStatus,
)
from src.utils.config import AppConfig


# =============================================================================
# Jobs de test — n'accedent pas a Spark
# =============================================================================

class SuccessJob(BaseJob):
    """Job qui reussit toujours (rien a faire)."""
    JOB_NAME = "success_job"

    def run(self) -> None:
        self.metrics.rows_written = 1


class FailingJob(BaseJob):
    """Job qui echoue toujours (simule une erreur reseau, source indisponible…)."""
    JOB_NAME = "failing_job"

    def run(self) -> None:
        raise RuntimeError("Source indisponible : connection refused")


def _mock_spark() -> MagicMock:
    """Cree un faux SparkSession pour les tests unitaires."""
    return MagicMock(name="SparkSession")


def _config() -> AppConfig:
    """Config minimale pour les tests."""
    return AppConfig()


# =============================================================================
# Tests de StepResult et StepStatus
# =============================================================================

@pytest.mark.unit
class TestStepResult:
    """Tests des dataclasses StepResult et StepStatus."""

    def test_step_result_default_elapsed(self) -> None:
        result = StepResult("my_step", StepStatus.SUCCESS)
        assert result.elapsed_seconds == 0.0

    def test_step_result_with_error(self) -> None:
        result = StepResult("my_step", StepStatus.FAILED, error="timeout")
        assert result.error == "timeout"
        assert result.status == StepStatus.FAILED

    def test_step_status_values(self) -> None:
        assert StepStatus.SUCCESS.value == "SUCCESS"
        assert StepStatus.FAILED.value  == "FAILED"
        assert StepStatus.SKIPPED.value == "SKIPPED"
        assert StepStatus.PENDING.value == "PENDING"
        assert StepStatus.RUNNING.value == "RUNNING"


# =============================================================================
# Tests de PipelineReport
# =============================================================================

@pytest.mark.unit
class TestPipelineReport:
    """Tests du rapport d'execution agregant les resultats de tous les steps."""

    def test_success_when_no_failures(self) -> None:
        results = [
            StepResult("step_a", StepStatus.SUCCESS),
            StepResult("step_b", StepStatus.SUCCESS),
        ]
        report = PipelineReport("test_pipeline", results)
        assert report.success is True

    def test_failure_when_one_step_failed(self) -> None:
        results = [
            StepResult("step_a", StepStatus.FAILED),
            StepResult("step_b", StepStatus.SKIPPED),
        ]
        report = PipelineReport("test_pipeline", results)
        assert report.success is False

    def test_counts_by_status(self) -> None:
        results = [
            StepResult("a", StepStatus.SUCCESS),
            StepResult("b", StepStatus.SUCCESS),
            StepResult("c", StepStatus.FAILED),
            StepResult("d", StepStatus.SKIPPED),
        ]
        report = PipelineReport("test_pipeline", results)
        assert len(report.steps_success) == 2
        assert len(report.steps_failed)  == 1
        assert len(report.steps_skipped) == 1

    def test_total_elapsed_sums_all_steps(self) -> None:
        results = [
            StepResult("a", StepStatus.SUCCESS, elapsed_seconds=1.5),
            StepResult("b", StepStatus.SUCCESS, elapsed_seconds=2.5),
        ]
        report = PipelineReport("test_pipeline", results)
        assert report.total_elapsed == pytest.approx(4.0)

    def test_empty_pipeline_is_success(self) -> None:
        """Un pipeline sans steps = succes (aucun echec possible)."""
        report = PipelineReport("empty", [])
        assert report.success is True

    def test_print_summary_does_not_raise(self, capsys) -> None:
        """print_summary() doit s'executer sans erreur."""
        results = [
            StepResult("step_a", StepStatus.SUCCESS),
            StepResult("step_b", StepStatus.FAILED, error="boom"),
            StepResult("step_c", StepStatus.SKIPPED),
        ]
        report = PipelineReport("demo", results)
        report.print_summary()   # Ne doit pas lever d'exception
        captured = capsys.readouterr()
        assert "demo" in captured.out
        assert "ECHEC" in captured.out


# =============================================================================
# Tests de PipelineStep
# =============================================================================

@pytest.mark.unit
class TestPipelineStep:
    """Tests de la definition d'un step (PipelineStep dataclass)."""

    def test_default_no_dependencies(self) -> None:
        step = PipelineStep("my_step", SuccessJob, config=_config())
        assert step.dependencies == []
        assert step.retry_on_fail is False

    def test_with_dependencies(self) -> None:
        step = PipelineStep("silver", SuccessJob,
                            dependencies=["bronze"], config=_config())
        assert "bronze" in step.dependencies

    def test_retry_on_fail_flag(self) -> None:
        step = PipelineStep("flaky", SuccessJob,
                            retry_on_fail=True, config=_config())
        assert step.retry_on_fail is True


# =============================================================================
# Tests de Pipeline — logique DAG (sans Spark reel)
# =============================================================================

@pytest.mark.unit
class TestPipelineDagLogic:
    """
    Tests de la logique DAG du Pipeline.

    Un MagicMock de SparkSession est suffisant ici.
    Les jobs (SuccessJob/FailingJob) n'accedent pas a self.spark
    donc aucune JVM n'est demarree.
    """

    def test_nominal_single_step_success(self) -> None:
        """Un pipeline avec un seul step qui reussit."""
        pipeline = (
            Pipeline("test_single", spark=_mock_spark())
            .add_step(PipelineStep("step_a", SuccessJob, config=_config()))
        )
        report = pipeline.run()
        assert report.success
        assert len(report.steps_success) == 1

    def test_nominal_all_steps_success(self) -> None:
        """Tous les steps reussissent -> pipeline SUCCESS."""
        pipeline = (
            Pipeline("test_nominal", spark=_mock_spark())
            .add_step(PipelineStep("step_a", SuccessJob, config=_config()))
            .add_step(PipelineStep("step_b", SuccessJob, config=_config()))
            .add_step(PipelineStep("step_c", SuccessJob, config=_config()))
        )
        report = pipeline.run()
        assert report.success
        assert len(report.steps_success) == 3
        assert len(report.steps_failed)  == 0
        assert len(report.steps_skipped) == 0

    def test_step_skipped_when_dependency_fails(self) -> None:
        """
        Si step_a echoue et que step_b depend de step_a,
        step_b doit passer en SKIPPED (pas execute).

        C'est le mecanisme "fail-fast" vu dans la PARTIE C de run_pipeline.py.
        """
        pipeline = (
            Pipeline("test_skip", spark=_mock_spark())
            .add_step(PipelineStep("step_a", FailingJob, config=_config()))
            .add_step(PipelineStep(
                "step_b", SuccessJob,
                dependencies=["step_a"],
                config=_config(),
            ))
        )
        report = pipeline.run()
        assert not report.success
        assert report.steps_failed[0].step_name  == "step_a"
        assert report.steps_skipped[0].step_name == "step_b"

    def test_fail_fast_skips_all_remaining_steps(self) -> None:
        """
        Quand step_a echoue, les steps suivants (b et c) sont tous SKIPPED.
        Le pipeline s'arrete apres le premier echec (fail-fast).
        """
        pipeline = (
            Pipeline("test_failfast", spark=_mock_spark())
            .add_step(PipelineStep("a", FailingJob, config=_config()))
            .add_step(PipelineStep("b", SuccessJob,
                                   dependencies=["a"], config=_config()))
            .add_step(PipelineStep("c", SuccessJob,
                                   dependencies=["b"], config=_config()))
        )
        report = pipeline.run()
        assert len(report.steps_failed)  == 1
        assert len(report.steps_skipped) == 2
        assert not report.success

    def test_independent_step_not_skipped_after_failure(self) -> None:
        """
        Un step SANS dependance sur le step qui a echoue
        est quand meme SKIPPED (fail-fast global).

        Note : notre Pipeline implemente un fail-fast strict :
        tout le pipeline s'arrete a la premiere erreur,
        meme les steps independants des suivants.
        """
        pipeline = (
            Pipeline("test_independent", spark=_mock_spark())
            .add_step(PipelineStep("a", FailingJob, config=_config()))
            .add_step(PipelineStep("b", SuccessJob, config=_config()))  # pas de dep sur a
        )
        report = pipeline.run()
        # b est quand meme SKIPPED a cause du fail-fast global
        assert len(report.steps_skipped) == 1
        assert report.steps_skipped[0].step_name == "b"

    def test_step_skipped_when_missing_dependency(self) -> None:
        """
        Un step dont la dependance declaree n'existe pas dans le pipeline
        est passe en SKIPPED (dependance non satisfaite).
        """
        pipeline = (
            Pipeline("test_missing_dep", spark=_mock_spark())
            .add_step(PipelineStep(
                "orphan", SuccessJob,
                dependencies=["nonexistent_step"],
                config=_config(),
            ))
        )
        report = pipeline.run()
        assert len(report.steps_skipped) == 1
        assert report.steps_skipped[0].step_name == "orphan"

    def test_retry_on_fail_retries_once(self) -> None:
        """
        Un step avec retry_on_fail=True est rejoue automatiquement
        a la premiere erreur transitoire.

        Simule le scenario PARTIE D de run_pipeline.py :
          Tentative 1 : echec
          Tentative 2 : succes
        """
        attempt_counter = {"n": 0}

        class FlakyJob(BaseJob):
            JOB_NAME = "flaky"

            def run(self) -> None:
                attempt_counter["n"] += 1
                if attempt_counter["n"] == 1:
                    raise RuntimeError("Erreur transitoire")
                # 2eme tentative : succes
                self.metrics.rows_written = 42

        pipeline = (
            Pipeline("test_retry", spark=_mock_spark())
            .add_step(PipelineStep(
                "flaky_step", FlakyJob,
                retry_on_fail=True,
                config=_config(),
            ))
        )
        report = pipeline.run()
        assert report.success
        assert attempt_counter["n"] == 2   # exactement 2 tentatives

    def test_retry_exhausted_marks_step_failed(self) -> None:
        """
        Si le step echoue aux deux tentatives, il est marque FAILED.
        """
        pipeline = (
            Pipeline("test_retry_fail", spark=_mock_spark())
            .add_step(PipelineStep(
                "always_fails", FailingJob,
                retry_on_fail=True,   # 2 tentatives, les deux echouent
                config=_config(),
            ))
        )
        report = pipeline.run()
        assert not report.success
        assert len(report.steps_failed) == 1

    def test_pipeline_add_step_returns_self_for_chaining(self) -> None:
        """add_step() retourne self -> permet le chaining fluent."""
        pipeline = Pipeline("chain_test", spark=_mock_spark())
        result = pipeline.add_step(PipelineStep("a", SuccessJob, config=_config()))
        assert result is pipeline   # meme objet

    def test_step_result_contains_metrics(self) -> None:
        """Apres un run reussi, le StepResult contient les metriques du job."""
        pipeline = (
            Pipeline("test_metrics", spark=_mock_spark())
            .add_step(PipelineStep("step_a", SuccessJob, config=_config()))
        )
        report = pipeline.run()
        result = report.steps_success[0]
        assert result.metrics is not None
        assert result.metrics.rows_written == 1

    def test_step_result_contains_error_message_on_failure(self) -> None:
        """Apres un echec, le StepResult contient le message d'erreur."""
        pipeline = (
            Pipeline("test_error_msg", spark=_mock_spark())
            .add_step(PipelineStep("failing", FailingJob, config=_config()))
        )
        report = pipeline.run()
        result = report.steps_failed[0]
        assert result.error is not None
        assert "connection refused" in result.error
