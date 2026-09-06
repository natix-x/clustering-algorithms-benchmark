from __future__ import annotations

from utils.logger import get_logger
import os

from slurm_experiments_orchestrator.common.experiments_generator import Experiment
from slurm_experiments_orchestrator.common.config import SPARK_RESOURCE_KEYS
from slurm_experiments_orchestrator.launchers.base_launcher import Launcher, evaluation_block
from slurm_experiments_orchestrator.launchers.spark_resources_resolver import (
    SparkResources,
    resources_from_cell,
)
from slurm_experiments_orchestrator.launchers.sbatch_templates.spark_sbatch_template import SBATCH_TEMPLATE

logger = get_logger(__name__)


def _resolve_resources(experiment: Experiment) -> SparkResources:
    return resources_from_cell(experiment.cell["resources"])


class SparkLauncher(Launcher):
    name = "spark"
    resource_keys = SPARK_RESOURCE_KEYS

    def build_run_config(
        self, experiment: Experiment, output_dir: str, yaml_config: dict
    ) -> dict:
        spark_conf = dict(yaml_config.get("spark_config", {}))
        evaluation = evaluation_block(yaml_config, experiment)
        sbatch_config = yaml_config.get("sbatch_config", {})

        resources = experiment.cell["resources"]
        spark_resources = _resolve_resources(experiment)
        num_partitions = (
            experiment.nodes * int(resources["executors_per_node"]) * spark_resources.executor_cores
        )
        logger.debug(
            f"[spark] {experiment.run_id}: {spark_resources.executors_per_node} exec/node "
            f"x {spark_resources.executor_cores} cores, {num_partitions} partitions"
        )

        experiment_metadata = {
            "nodes": str(experiment.nodes),
            "cpus_per_task": str(resources["cpus_per_task"]),
            "executors_per_node": str(resources["executors_per_node"]),
            "total_executors": str(experiment.nodes * int(resources["executors_per_node"])),
            "mem": str(resources["mem"]),
            "walltime": str(sbatch_config.get("walltime", "")),
            "partition": str(sbatch_config.get("partition", "")),
            "spark_module": str(sbatch_config.get("spark_module", "")),
            "worker_mem_gb": str(spark_resources.worker_pool_gb),
            "executor_mem_gb": str(spark_resources.executor_gb),
            "executor_overhead_mb": str(spark_resources.executor_overhead_mb),
            "executor_cores": str(spark_resources.executor_cores),
            "driver_mem_gb": str(spark_resources.driver_gb),
            "master_mem_gb": str(spark_resources.master_gb),
            "num_partitions": str(num_partitions),
        }

        # Inject the derived partition count (contract: dataset.params.numPartitions — synthetic
        # generates that many, parquet re-partitions after the read). An explicit value wins, so a
        # partition-strategy sweep can pin its own count.
        dataset = dict(experiment.cell["datasets"])
        dataset["params"] = {"numPartitions": num_partitions, **dataset.get("params", {})}

        return {
            "runId": experiment.run_id,
            "profile": "ares",
            "outputDir": output_dir,
            "dataset": dataset,
            "algorithm": dict(experiment.cell["algorithms"]),
            "evaluation": evaluation,
            "spark_config": spark_conf,
            "experimentMetadata": experiment_metadata,
        }

    def render_sbatch(
        self,
        experiment: Experiment,
        run_config_path: str,
        log_dir: str,
        output_dir: str,
        yaml_config: dict,
    ) -> str:
        sbatch_config = yaml_config.get("sbatch_config", {})
        non_resource_defaults = {
            k: v for k, v in sbatch_config.items()
            if k not in ("executors_per_node", "cpus_per_task", "mem")
        }
        jar_path = os.path.expandvars(yaml_config["jar_path"])
        resources = experiment.cell["resources"]
        spark_resources = _resolve_resources(experiment)

        return SBATCH_TEMPLATE.format(
            name=yaml_config["name"],
            nodes=experiment.nodes,
            run_id=experiment.run_id,
            config_path=run_config_path,
            log_dir=log_dir,
            output_dir=output_dir,
            jar_path=jar_path,
            cpus_per_task=int(resources["cpus_per_task"]),
            mem=resources["mem"],
            executors_per_node=int(resources["executors_per_node"]),
            worker_mem=spark_resources.worker_pool_gb,
            driver_mem=spark_resources.driver_gb,
            master_mem=spark_resources.master_gb,
            executor_mem=spark_resources.executor_gb,
            executor_cores=spark_resources.executor_cores,
            executor_overhead_mb=spark_resources.executor_overhead_mb,
            **non_resource_defaults,
        )
