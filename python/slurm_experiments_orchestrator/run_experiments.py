from __future__ import annotations

import argparse
import logging
import os
import subprocess
from pathlib import Path

import yaml

from slurm_experiments_orchestrator import launchers
from slurm_experiments_orchestrator.common.experiments_generator import generate_experiments
from slurm_experiments_orchestrator.common.yaml_validator import validate_yaml_config_file
from slurm_experiments_orchestrator.slurm.jobs_writer import JobWriter

logging.basicConfig(level=logging.DEBUG, format="%(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate SLURM jobs for clustering benchmarks (Spark or Flink)."
    )
    parser.add_argument(
        "--framework", "-f",
        required=True,
        choices=launchers.available(),
        help="Execution engine to target.",
    )
    parser.add_argument("matrix", type=Path, help="Path to the YAML matrix file.")
    parser.add_argument(
        "--submit",
        action="store_true",
        help="After generating, run the sbatch submit_all.sh (submits all jobs).",
    )
    args = parser.parse_args()

    if not args.matrix.exists():
        raise FileNotFoundError(f"YAML config file not found: {args.matrix}")

    logger.info(f"Framework: {args.framework}  |  matrix: {args.matrix}")
    launcher = launchers.get_launcher(args.framework)

    yaml_config = yaml.safe_load(args.matrix.read_text())
    validate_yaml_config_file(yaml_config, resource_keys=launcher.resource_keys)
    logger.info("YAML config valid.")

    cfg_dir = os.path.expandvars(yaml_config["configs_dir"])
    log_dir = os.path.expandvars(yaml_config["log_dir"])
    output_dir = os.path.expandvars(yaml_config["output_dir"])
    for d in (cfg_dir, log_dir, output_dir):
        Path(d).mkdir(parents=True, exist_ok=True)
    logger.debug(f"Output dirs ready: configs={cfg_dir}, logs={log_dir}, output={output_dir}")

    repetitions = int(yaml_config.get("repetitions"))
    experiments = generate_experiments(yaml_config, repetitions)
    if repetitions > 1:
        logger.info(f"Repetitions: {repetitions}x -> {len(experiments)} runs in total")

    writer = JobWriter(launcher)
    writer.write_configs(experiments, cfg_dir, output_dir, yaml_config)

    sbatch_dir = str(Path(output_dir) / "sbatch")
    writer.write_sbatch_files(experiments, cfg_dir, log_dir, output_dir, sbatch_dir, yaml_config)

    submit_all = Path(sbatch_dir) / "submit_all.sh"
    logger.info(f"[{launcher.name}] Generated {len(experiments)} independent jobs.")

    if args.submit:
        logger.info(f"Submitting: {submit_all}")
        subprocess.run(["bash", str(submit_all)], check=True)
        logger.info("All jobs submitted. Check with: squeue -u $USER")
    else:
        logger.info(f"To submit all jobs:  bash {submit_all}")


if __name__ == "__main__":
    main()
