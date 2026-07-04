from __future__ import annotations

from abc import ABC, abstractmethod

from slurm_experiments_orchestrator.common.experiments_generator import Experiment


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
