from __future__ import annotations

from utils.logger import get_logger
import os

from slurm_experiments_orchestrator.common.experiments_generator import Experiment
from slurm_experiments_orchestrator.common.config import FLINK_RESOURCE_KEYS
from slurm_experiments_orchestrator.launchers.base_launcher import Launcher, evaluation_block
from slurm_experiments_orchestrator.launchers.flink_resources_resolver import (
    FlinkResources,
    resources_from_cell,
)
from slurm_experiments_orchestrator.launchers.sbatch_templates.flink_sbatch_template import FLINK_SBATCH_TEMPLATE

logger = get_logger(__name__)


def _resolve_resources(experiment: Experiment) -> FlinkResources:
    return resources_from_cell(experiment.cell["resources"])


class FlinkLauncher(Launcher):
    name = "flink"
    resource_keys = FLINK_RESOURCE_KEYS

    def build_run_config(
        self, experiment: Experiment, output_dir: str, yaml_config: dict
    ) -> dict:
        engine_conf = dict(yaml_config.get("flink_config", {}))
        evaluation = evaluation_block(yaml_config, experiment)
        sbatch_config = yaml_config.get("sbatch_config", {})
        resources = experiment.cell["resources"]

        flink_resources = _resolve_resources(experiment)
        tms_per_node = flink_resources.tms_per_node
        slots_per_tm = flink_resources.slots_per_tm
        parallelism = experiment.nodes * tms_per_node * slots_per_tm
        logger.debug(
            f"[flink] {experiment.run_id}: {experiment.nodes * tms_per_node} TM "
            f"x {slots_per_tm} slots, parallelism {parallelism}"
        )

        experiment_metadata = {
            "nodes": str(experiment.nodes),
            "cpus_per_task": str(resources["cpus_per_task"]),
            "tm_per_node": str(tms_per_node),
            "total_taskmanagers":str(experiment.nodes * tms_per_node),
            "slots_per_taskmanager": str(slots_per_tm),
            "parallelism": str(parallelism),
            "mem": str(resources["mem"]),
            "tm_mem_gb": str(resources["tm_mem_gb"]),
            "jm_mem_gb": str(resources["jm_mem_gb"]),
            "tm_process_mb": str(flink_resources.tm_process_mb),
            "tm_network_max_mb": str(flink_resources.tm_network_max_mb),
            "jm_process_mb": str(flink_resources.jm_process_mb),
            "jm_heap_mb": str(flink_resources.jm_heap_mb),
            "num_partitions": str(parallelism),
            "walltime": str(sbatch_config.get("walltime", "")),
            "partition": str(sbatch_config.get("partition", "")),
            "flink_module": str(sbatch_config.get("flink_module", "")),
        }

        # Always overwritten, for every dataset type: FlinkClusteringJob reads
        # dataset.params.numPartitions as the job parallelism, not as a source knob.
        dataset = dict(experiment.cell["datasets"])
        dataset["params"] = {**dataset.get("params", {}), "numPartitions": parallelism}

        return {
            "runId":              experiment.run_id,
            "profile":            "ares",
            "outputDir":          output_dir,
            "dataset":            dataset,
            "algorithm":          dict(experiment.cell["algorithms"]),
            "evaluation":         evaluation,
            "flink_config":       engine_conf,
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
        sbatch_def = yaml_config.get("sbatch_config", {})
        non_resource_defaults = {
            k: v for k, v in sbatch_def.items()
            if k not in self.resource_keys
        }
        jar_path = os.path.expandvars(yaml_config["jar_path"])
        resources = experiment.cell["resources"]
        flink_resources = _resolve_resources(experiment)
        tms_per_node = flink_resources.tms_per_node
        slots_per_tm = flink_resources.slots_per_tm
        num_partitions = experiment.nodes * tms_per_node * slots_per_tm  # = total slots

        return FLINK_SBATCH_TEMPLATE.format(
            name=yaml_config["name"],
            nodes=experiment.nodes,
            run_id=experiment.run_id,
            config_path=run_config_path,
            log_dir=log_dir,
            output_dir=output_dir,
            jar_path=jar_path,
            mem=resources["mem"],
            tms_per_node=tms_per_node,
            slots_per_tm=slots_per_tm,
            tm_process_mb=flink_resources.tm_process_mb,
            tm_network_max_mb=flink_resources.tm_network_max_mb,
            jm_process_mb=flink_resources.jm_process_mb,
            jm_heap_mb=flink_resources.jm_heap_mb,
            parallelism=num_partitions,
            **non_resource_defaults,
        )
