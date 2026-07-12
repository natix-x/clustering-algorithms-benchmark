from __future__ import annotations

import logging
import os

from slurm_experiments_orchestrator.common.experiments_generator import Experiment
from slurm_experiments_orchestrator.common.config import SPARK_RESOURCE_KEYS
from slurm_experiments_orchestrator.launchers.base_launcher import Launcher
from slurm_experiments_orchestrator.launchers.spark_resources_resolver import (
    SparkResources,
    resources_from_cell,
)
from slurm_experiments_orchestrator.launchers.sbatch_templates.spark_sbatch_template import SBATCH_TEMPLATE

logger = logging.getLogger(__name__)


def _resolve_resources(experiment: Experiment) -> SparkResources:
    return resources_from_cell(experiment.cell["resources"])


class SparkLauncher(Launcher):
    name = "spark"
    resource_keys = SPARK_RESOURCE_KEYS

    def build_run_config(
        self, experiment: Experiment, output_dir: str, yaml_config: dict
    ) -> dict:
        spark_conf      = dict(yaml_config.get("spark_config", {}))
        evaluation      = dict(yaml_config.get("evaluation_config", {}))
        sbatch_config = yaml_config.get("sbatch_config", {})

        res       = experiment.cell["resources"]
        spark_res = _resolve_resources(experiment)
        num_partitions = (
            experiment.nodes * int(res["executors_per_node"]) * spark_res.executor_cores
        )
        logger.debug(
            f"[spark] {experiment.run_id}: {spark_res.executors_per_node} exec/node "
            f"x {spark_res.executor_cores} cores, {num_partitions} partitions"
        )

        experiment_metadata = {
            "nodes":              str(experiment.nodes),
            "cpus_per_task":      str(res["cpus_per_task"]),
            "executors_per_node": str(res["executors_per_node"]),
            "total_executors":    str(experiment.nodes * int(res["executors_per_node"])),
            "mem":                str(res["mem"]),
            "walltime":           str(sbatch_config.get("walltime", "")),
            "partition":          str(sbatch_config.get("partition", "")),
            "spark_module":       str(sbatch_config.get("spark_module", "")),
            "worker_mem_gb":      str(spark_res.worker_pool_gb),
            "executor_mem_gb":    str(spark_res.executor_gb),
            "executor_overhead_mb": str(spark_res.executor_overhead_mb),
            "executor_cores":     str(spark_res.executor_cores),
            "driver_mem_gb":      str(spark_res.driver_gb),
            "master_mem_gb":      str(spark_res.master_gb),
            "num_partitions":     str(num_partitions),
        }

        # Inject the derived partition count into the dataset params so the Spark job
        # reads it (contract: dataset.params.numPartitions).
        dataset = dict(experiment.cell["datasets"])
        dataset["params"] = {**dataset.get("params", {}), "numPartitions": num_partitions}

        return {
            "runId":              experiment.run_id,
            "profile":            "ares",
            "outputDir":          output_dir,
            "dataset":            dataset,
            "algorithm":          dict(experiment.cell["algorithms"]),
            "evaluation":         evaluation,
            "spark_config":       spark_conf,
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
        jar       = os.path.expandvars(yaml_config["jar_path"])
        res       = experiment.cell["resources"]
        spark_res = _resolve_resources(experiment)

        return SBATCH_TEMPLATE.format(
            name=yaml_config["name"],
            nodes=experiment.nodes,
            run_id=experiment.run_id,
            config_path=run_config_path,
            log_dir=log_dir,
            output_dir=output_dir,
            jar=jar,
            cpus_per_task=int(res["cpus_per_task"]),
            mem=res["mem"],
            executors_per_node=int(res["executors_per_node"]),
            worker_mem=spark_res.worker_pool_gb,
            driver_mem=spark_res.driver_gb,
            executor_mem=spark_res.executor_gb,
            executor_cores=spark_res.executor_cores,
            executor_overhead_mb=spark_res.executor_overhead_mb,
            **non_resource_defaults,
        )
