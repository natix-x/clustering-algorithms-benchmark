"""Launcher factory, build_run_config derivations, and render_sbatch output."""
from __future__ import annotations

import pytest

from slurm_experiments_orchestrator import launchers
from slurm_experiments_orchestrator.launchers.flink_launcher import FlinkLauncher
from slurm_experiments_orchestrator.launchers.spark_launcher import SparkLauncher
from slurm_experiments_orchestrator.common.experiments_generator import generate_experiments


# --- factory ---

def test_available_lists_both():
    assert launchers.available() == ["flink", "spark"]


def test_get_launcher_returns_instances():
    assert isinstance(launchers.get_launcher("spark"), SparkLauncher)
    assert isinstance(launchers.get_launcher("flink"), FlinkLauncher)


def test_get_launcher_case_insensitive():
    assert isinstance(launchers.get_launcher("SPARK"), SparkLauncher)


def test_unknown_framework_raises():
    with pytest.raises(ValueError):
        launchers.get_launcher("dask")


# --- build_run_config: derivations + injection ---

def test_spark_partitions_equal_total_cores(spark_yaml):
    # nodes 1 x executors 1 x executor_cores(=cpus/exec=4) = 4
    exp = generate_experiments(spark_yaml, 1)[0]
    cfg = launchers.get_launcher("spark").build_run_config(exp, "/tmp/o", spark_yaml)
    assert cfg["dataset"]["params"]["numPartitions"] == 4
    assert cfg["experimentMetadata"]["num_partitions"] == "4"


def test_flink_parallelism_equals_total_slots(flink_yaml):
    # nodes 1 x tm_per_node 1 x cpus_per_task 4 = 4
    exp = generate_experiments(flink_yaml, 1)[0]
    cfg = launchers.get_launcher("flink").build_run_config(exp, "/tmp/o", flink_yaml)
    assert cfg["dataset"]["params"]["numPartitions"] == 4
    assert cfg["experimentMetadata"]["parallelism"] == "4"


def test_spark_uses_sparkconf_key(spark_yaml):
    exp = generate_experiments(spark_yaml, 1)[0]
    cfg = launchers.get_launcher("spark").build_run_config(exp, "/tmp/o", spark_yaml)
    assert "sparkConf" in cfg and "engineConf" not in cfg


def test_flink_uses_engineconf_key(flink_yaml):
    exp = generate_experiments(flink_yaml, 1)[0]
    cfg = launchers.get_launcher("flink").build_run_config(exp, "/tmp/o", flink_yaml)
    assert "engineConf" in cfg and "sparkConf" not in cfg


def test_experiment_metadata_all_strings(spark_yaml):
    exp = generate_experiments(spark_yaml, 1)[0]
    cfg = launchers.get_launcher("spark").build_run_config(exp, "/tmp/o", spark_yaml)
    assert all(isinstance(v, str) for v in cfg["experimentMetadata"].values())


def test_build_does_not_mutate_original_dataset(spark_yaml):
    exp = generate_experiments(spark_yaml, 1)[0]
    launchers.get_launcher("spark").build_run_config(exp, "/tmp/o", spark_yaml)
    # numPartitions injected into a copy, not the source cell
    assert "numPartitions" not in exp.cell["datasets"]["params"]


# --- render_sbatch ---

def test_spark_render_has_key_directives(spark_yaml):
    exp = generate_experiments(spark_yaml, 1)[0]
    sb = launchers.get_launcher("spark").render_sbatch(
        experiment=exp, run_config_path="/tmp/c.json",
        log_dir="/tmp/l", output_dir="/tmp/o", yaml_config=spark_yaml,
    )
    assert sb.startswith("#!/bin/bash")
    assert "#SBATCH -N 1" in sb
    assert "spark.serializer=org.apache.spark.serializer.KryoSerializer" in sb
    assert "spark-submit" in sb


def test_flink_render_has_parallelism(flink_yaml):
    exp = generate_experiments(flink_yaml, 1)[0]
    sb = launchers.get_launcher("flink").render_sbatch(
        experiment=exp, run_config_path="/tmp/c.json",
        log_dir="/tmp/l", output_dir="/tmp/o", yaml_config=flink_yaml,
    )
    assert 'parallelism.default: 4' in sb
    assert '-p "4"' in sb
