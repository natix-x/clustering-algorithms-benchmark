from __future__ import annotations

import copy

import pytest


def _base_yaml(matrix: dict, sbatch_defaults: dict) -> dict:
    return {
        "name": "t",
        "configs_dir": "c",
        "log_dir": "l",
        "output_dir": "o",
        "jar_path": "j",
        "repetitions": 1,
        "sbatch_defaults": sbatch_defaults,
        "matrix": matrix,
    }


_SPARK_SBATCH = {
    "account": "acc", "partition": "plgrid", "walltime": "00:10:00",
    "spark_module": "spark/3.3.2",
}
_FLINK_SBATCH = {
    "account": "acc", "partition": "plgrid", "walltime": "00:10:00",
    "java_module": "java/11", "flink_home": "/scratch/flink",
}


_SPARK_MATRIX = {
    "nodes": [1],
    "resources": [{"cpus_per_task": 4, "executors_per_node": 1, "mem": "8G",
                   "driver_mem_gb": 2, "worker_mem_gb": 5}],
    "algorithm": [{"name": "kmeans", "params": {"k": 3}}],
    "dataset": [{"type": "synthetic", "params": {"numPoints": 100}}],
}

_FLINK_MATRIX = {
    "nodes": [1],
    "resources": [{"cpus_per_task": 4, "tm_per_node": 1, "mem": "8G",
                   "tm_mem_gb": 6, "jm_mem_gb": 1}],
    "algorithm": [{"name": "kmeans", "params": {"k": 3}}],
    "dataset": [{"type": "synthetic", "params": {"numPoints": 100}}],
}


@pytest.fixture
def spark_yaml() -> dict:
    """A minimal, valid Spark matrix config (fresh copy per test)."""
    return _base_yaml(copy.deepcopy(_SPARK_MATRIX), copy.deepcopy(_SPARK_SBATCH))


@pytest.fixture
def flink_yaml() -> dict:
    """A minimal, valid Flink matrix config (fresh copy per test)."""
    return _base_yaml(copy.deepcopy(_FLINK_MATRIX), copy.deepcopy(_FLINK_SBATCH))
