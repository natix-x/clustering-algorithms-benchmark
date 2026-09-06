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
    exp = generate_experiments(spark_yaml)[0]
    cfg = launchers.get_launcher("spark").build_run_config(exp, "/tmp/o", spark_yaml)
    assert cfg["dataset"]["params"]["numPartitions"] == 4
    assert cfg["experimentMetadata"]["num_partitions"] == "4"


def test_flink_parallelism_equals_total_slots(flink_yaml):
    # nodes 1 x tm_per_node 1 x cpus_per_task 4 = 4
    exp = generate_experiments(flink_yaml)[0]
    cfg = launchers.get_launcher("flink").build_run_config(exp, "/tmp/o", flink_yaml)
    assert cfg["dataset"]["params"]["numPartitions"] == 4
    assert cfg["experimentMetadata"]["parallelism"] == "4"


def test_spark_uses_spark_config_key(spark_yaml):
    exp = generate_experiments(spark_yaml)[0]
    cfg = launchers.get_launcher("spark").build_run_config(exp, "/tmp/o", spark_yaml)
    assert "spark_config" in cfg and "flink_config" not in cfg


def test_flink_uses_flink_config_key(flink_yaml):
    exp = generate_experiments(flink_yaml)[0]
    cfg = launchers.get_launcher("flink").build_run_config(exp, "/tmp/o", flink_yaml)
    assert "flink_config" in cfg and "spark_config" not in cfg


def test_experiment_metadata_all_strings(spark_yaml):
    exp = generate_experiments(spark_yaml)[0]
    cfg = launchers.get_launcher("spark").build_run_config(exp, "/tmp/o", spark_yaml)
    assert all(isinstance(v, str) for v in cfg["experimentMetadata"].values())


def test_build_does_not_mutate_original_dataset(spark_yaml):
    exp = generate_experiments(spark_yaml)[0]
    launchers.get_launcher("spark").build_run_config(exp, "/tmp/o", spark_yaml)
    # numPartitions injected into a copy, not the source cell
    assert "numPartitions" not in exp.cell["datasets"]["params"]


# --- render_sbatch ---

def test_spark_render_has_key_directives(spark_yaml):
    exp = generate_experiments(spark_yaml)[0]
    sb = launchers.get_launcher("spark").render_sbatch(
        experiment=exp, run_config_path="/tmp/c.json",
        log_dir="/tmp/l", output_dir="/tmp/o", yaml_config=spark_yaml,
    )
    assert sb.startswith("#!/bin/bash")
    assert "#SBATCH -N 1" in sb
    assert "spark.serializer=org.apache.spark.serializer.KryoSerializer" in sb
    assert "spark-submit" in sb


def test_flink_render_has_parallelism(flink_yaml):
    exp = generate_experiments(flink_yaml)[0]
    sb = launchers.get_launcher("flink").render_sbatch(
        experiment=exp, run_config_path="/tmp/c.json",
        log_dir="/tmp/l", output_dir="/tmp/o", yaml_config=flink_yaml,
    )
    assert 'parallelism.default: 4' in sb
    # Application Mode: parallelism comes only from flink-conf.yaml above (no `flink run -p`
    # flag anymore — standalone-job.sh runs main() inside the JobManager process itself).
    assert '--job-classname clustering.benchmark.BenchmarkRunner' in sb


def test_spark_parquet_gets_the_derived_partition_count(parquet_yaml):
    exp = generate_experiments(parquet_yaml)[0]
    params = launchers.get_launcher("spark").build_run_config(exp, "/tmp/o", parquet_yaml)["dataset"]["params"]
    assert params["numPartitions"] == 4
    assert params["featureColumnName"] == "emb"


def test_spark_explicit_partition_count_is_kept(parquet_yaml):
    parquet_yaml["experiment_matrix"]["datasets"][0]["params"]["numPartitions"] = 128
    exp = generate_experiments(parquet_yaml)[0]
    params = launchers.get_launcher("spark").build_run_config(exp, "/tmp/o", parquet_yaml)["dataset"]["params"]
    assert params["numPartitions"] == 128


# --- evaluation seed: per-repetition, not per-run and not constant ---

def test_evaluation_seed_advances_with_repetition(spark_yaml):
    """A frozen eval seed makes every repetition draw the SAME silhouette subsample, so a
    deterministic algorithm reports sd = 0 across repetitions — the estimator's own sampling
    variance held fixed rather than shown to be absent. `seed + rep` is what makes a repetition
    sweep measure something."""
    spark_yaml["repetitions"] = 3
    spark_yaml["evaluation_config"] = {"metrics": ["silhouette"], "sampleSize": 10000, "seed": 42}

    launcher = launchers.get_launcher("spark")
    seeds = [
        launcher.build_run_config(exp, "/tmp/o", spark_yaml)["evaluation"]["seed"]
        for exp in generate_experiments(spark_yaml)
    ]

    assert sorted(seeds) == [42, 43, 44], seeds


def test_evaluation_seed_is_shared_across_cells_of_one_repetition(spark_yaml):
    """The seed depends ONLY on the repetition, so within one repetition every algorithm, node
    count and resource cell draws the identical sample — cross-algorithm comparisons stay paired
    and the sample still does not move with worker count. Deriving it from run_id would break
    both."""
    spark_yaml["repetitions"] = 2
    spark_yaml["experiment_matrix"]["algorithms"] = [
        {"name": "kmeans", "params": {"k": 3}},
        {"name": "bisectingkmeans", "params": {"k": 3}},
    ]
    spark_yaml["evaluation_config"] = {"seed": 7}

    launcher = launchers.get_launcher("spark")
    by_rep: dict[int, set[int]] = {}
    for exp in generate_experiments(spark_yaml):
        cfg = launcher.build_run_config(exp, "/tmp/o", spark_yaml)
        by_rep.setdefault(exp.rep, set()).add(cfg["evaluation"]["seed"])

    assert by_rep == {0: {7}, 1: {8}}, by_rep


def test_evaluation_seed_defaults_to_the_engine_default(spark_yaml):
    """An omitted seed must not silently become 0: both engines default to 42, and a config that
    omits the key has to keep measuring the same thing."""
    spark_yaml["repetitions"] = 1
    spark_yaml.pop("evaluation_config", None)
    exp = generate_experiments(spark_yaml)[0]

    cfg = launchers.get_launcher("spark").build_run_config(exp, "/tmp/o", spark_yaml)
    assert cfg["evaluation"]["seed"] == 42


def test_flink_gets_the_same_seed_rule(flink_yaml):
    """Both engines must draw the same sample for the same repetition, or the cross-engine
    silhouette comparison has an extra confounder."""
    flink_yaml["repetitions"] = 2
    flink_yaml["evaluation_config"] = {"seed": 100}

    seeds = [
        launchers.get_launcher("flink").build_run_config(exp, "/tmp/o", flink_yaml)["evaluation"]["seed"]
        for exp in generate_experiments(flink_yaml)
    ]
    assert sorted(seeds) == [100, 101]
