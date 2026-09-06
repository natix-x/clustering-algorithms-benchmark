from __future__ import annotations

import json
from utils.logger import get_logger
from collections import defaultdict
from pathlib import Path

from slurm_experiments_orchestrator.common.experiments_generator import Experiment
from slurm_experiments_orchestrator.launchers.base_launcher import Launcher

logger = get_logger(__name__)


class JobWriter:
    def __init__(self, launcher: Launcher) -> None:
        self._launcher = launcher

    def write_configs(
            self,
            experiments: list[Experiment],
            cfg_dir: str,
            output_dir: str,
            yaml_config: dict,
    ) -> None:
        for experiment in experiments:
            config = self._launcher.build_run_config(experiment, output_dir, yaml_config)
            path = Path(cfg_dir) / f"{experiment.run_id}.json"
            path.write_text(json.dumps(config, indent=2, ensure_ascii=False))
            logger.debug(f"Wrote run-config {path}")
        logger.info(f"Wrote {len(experiments)} run-config JSON(s) to {cfg_dir}")

    def write_sbatch_files(
            self,
            experiments: list[Experiment],
            cfg_dir: str,
            log_dir: str,
            output_dir: str,
            sbatch_dir: str,
            yaml_config: dict,
    ) -> None:
        """Generate one sbatch script per experiment plus a submit_all.sh, grouped by node count."""
        sbatch_path = Path(sbatch_dir)
        sbatch_path.mkdir(parents=True, exist_ok=True)

        # Cap on how many runs of this matrix may execute at once. Unset = SLURM decides, which
        # let uncontrolled concurrency leak into the timings via Lustre contention and Flink's
        # reporter (file write+rename per tick vs Spark's RPC) — see git history for the numbers.
        max_concurrent = yaml_config.get("max_concurrent_runs")

        submit_lines = ["#!/bin/bash", "set -e", ""]
        if max_concurrent:
            submit_lines += [
                f"# Throttled to {max_concurrent} concurrent run(s): job i waits for job i-{max_concurrent}",
                "# (afterany, so one failure does not strand the rest of the matrix).",
                "IDS=()",
                "",
            ]

        submitted: list = []
        groups: dict[int, list[Experiment]] = defaultdict(list)
        for experiment in experiments:
            groups[experiment.nodes].append(experiment)

        for nodes in sorted(groups):
            for experiment in groups[nodes]:
                run_config_path = Path(cfg_dir) / f"{experiment.run_id}.json"
                script_path = sbatch_path / f"{experiment.run_id}.sbatch"

                script_path.write_text(self._launcher.render_sbatch(
                    experiment=experiment,
                    run_config_path=str(run_config_path),
                    log_dir=log_dir,
                    output_dir=output_dir,
                    yaml_config=yaml_config,
                ))
                script_path.chmod(0o755)
                if max_concurrent:
                    index = len(submitted)
                    if index < max_concurrent:
                        submit_lines.append(f"IDS+=( $(sbatch --parsable {script_path}) )")
                    else:
                        blocker = index - max_concurrent
                        submit_lines.append(
                            f"IDS+=( $(sbatch --parsable "
                            f"--dependency=afterany:${{IDS[{blocker}]}} {script_path}) )")
                    submitted.append(script_path)
                else:
                    submit_lines.append(f"sbatch {script_path}")

            logger.info(f"nodes={nodes}: {len(groups[nodes])} sbatch files generated.")

        submit_all_file = sbatch_path / "submit_all.sh"
        submit_all_file.write_text("\n".join(submit_lines) + "\n")
        submit_all_file.chmod(0o755)
        logger.info(f"Wrote submit-all script: {submit_all_file}")
