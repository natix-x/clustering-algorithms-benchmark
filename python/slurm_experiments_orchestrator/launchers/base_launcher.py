from __future__ import annotations

from abc import ABC, abstractmethod

from slurm_experiments_orchestrator.common.experiments_generator import Experiment

#: `evaluation_config.seed` when the YAML does not pin one. Matches the engines' own default,
#: so an omitted key measures the same thing everywhere.
DEFAULT_EVALUATION_SEED = 42


def evaluation_block(yaml_config: dict, experiment: Experiment) -> dict:
    """The per-run `evaluation` block: `evaluation_config` with the seed advanced by `rep`.

    A fixed seed would make every repetition draw the same silhouette subsample, reporting
    `sd = 0` — hiding the estimator's real sampling variance, which reproducibility is meant to
    measure. Advancing by `rep` (not by `run_id`) keeps the seed independent of algorithm/node
    count, so repetitions vary while cross-algorithm comparisons still draw the same sample.
    """
    evaluation = dict(yaml_config.get("evaluation_config", {}))
    base_seed = int(evaluation.get("seed", DEFAULT_EVALUATION_SEED))
    evaluation["seed"] = base_seed + experiment.rep
    return evaluation


class Launcher(ABC):
    name: str
    resource_keys: tuple[str, ...]

    @abstractmethod
    def build_run_config(
        self, experiment: Experiment, output_dir: str, yaml_config: dict
    ) -> dict:
        """Return the per-run config dict that validates against
        contract/run_config.schema.json."""

    @abstractmethod
    def render_sbatch(
        self,
        experiment: Experiment,
        run_config_path: str,
        log_dir: str,
        output_dir: str,
        yaml_config: dict,
    ) -> str:
        """Return the full sbatch script text for one experiment (cluster bootstrap +
        the engine submit command pointed at run_config_path)."""
