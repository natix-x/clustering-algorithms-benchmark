from __future__ import annotations

from utils.logger import get_logger
import os

from slurm_experiments_orchestrator.common.experiments_generator import Experiment
from slurm_experiments_orchestrator.common.config import FLINK_RESOURCE_KEYS
from slurm_experiments_orchestrator.launchers.base_launcher import Launcher
from slurm_experiments_orchestrator.launchers.sbatch_templates.flink_sbatch_template import FLINK_SBATCH_TEMPLATE

logger = get_logger(__name__)


class FlinkLauncher(Launcher):
    name = "flink"
    resource_keys = FLINK_RESOURCE_KEYS

    def build_run_config(
        self, experiment: Experiment, output_dir: str, yaml_config: dict
    ) -> dict:
        engine_conf = dict(yaml_config.get("flink_config", {}))
        evaluation  = dict(yaml_config.get("evaluation_config", {}))
        sbatch_def  = yaml_config.get("sbatch_config", {})
        res         = experiment.cell["resources"]

        tms_per_node = int(res["tm_per_node"])
        slots_per_tm = int(res["cpus_per_task"])
        parallelism = experiment.nodes * tms_per_node * slots_per_tm
        logger.debug(
            f"[flink] {experiment.run_id}: {experiment.nodes * tms_per_node} TM "
            f"x {slots_per_tm} slots, parallelism {parallelism}"
        )

        experiment_metadata = {
            "nodes":                 str(experiment.nodes),
            "cpus_per_task":         str(res["cpus_per_task"]),
            "tm_per_node":           str(tms_per_node),
            "total_taskmanagers":    str(experiment.nodes * tms_per_node),
            "slots_per_taskmanager": str(slots_per_tm),
            "parallelism":           str(parallelism),
            "mem":                   str(res["mem"]),
            "tm_mem_gb":             str(res["tm_mem_gb"]),
            "jm_mem_gb":             str(res["jm_mem_gb"]),
            "walltime":              str(sbatch_def.get("walltime", "")),
            "partition":             str(sbatch_def.get("partition", "")),
            "flink_module":          str(sbatch_def.get("flink_module", "")),
        }

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
        jar          = os.path.expandvars(yaml_config["jar_path"])
        res          = experiment.cell["resources"]
        tms_per_node = int(res["tm_per_node"])
        slots_per_tm = int(res["cpus_per_task"])
        num_partitions = experiment.nodes * tms_per_node * slots_per_tm  # = total slots

        return FLINK_SBATCH_TEMPLATE.format(
            name=yaml_config["name"],
            nodes=experiment.nodes,
            run_id=experiment.run_id,
            config_path=run_config_path,
            log_dir=log_dir,
            output_dir=output_dir,
            jar=jar,
            mem=res["mem"],
            tms_per_node=tms_per_node,
            slots_per_tm=slots_per_tm,
            tm_mem=int(res["tm_mem_gb"]),
            jm_mem=int(res["jm_mem_gb"]),
            parallelism=num_partitions,
            **non_resource_defaults,
        )
