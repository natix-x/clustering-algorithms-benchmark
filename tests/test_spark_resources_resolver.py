"""Spark resource packing math (cores/memory split across executors)."""
from __future__ import annotations

import pytest

from slurm_experiments_orchestrator.launchers.spark_resources_resolver import (
    SparkResources,
    resources_from_cell,
)


def _cell(**over):
    base = {"cpus_per_task": 8, "executors_per_node": 2, "mem": "48G",
            "driver_mem_gb": 4, "worker_mem_gb": 38}
    base.update(over)
    return base


def test_executor_cores_is_cpus_per_task_regardless_of_executors_per_node():
    # cpus_per_task is cores PER EXECUTOR (SLURM's own per-task unit) — it is not split
    # across executors_per_node. The Worker daemon instead advertises their combined
    # pool (see spark_sbatch_template.py's WORKER_CORES), so num_partitions / total
    # cores scale with executors_per_node, cores/executor stay pinned at cpus_per_task.
    r = resources_from_cell(_cell(cpus_per_task=8, executors_per_node=2))
    assert r.executor_cores == 8


def test_single_executor_gets_all_cores():
    r = resources_from_cell(_cell(cpus_per_task=8, executors_per_node=1, worker_mem_gb=38))
    assert r.executor_cores == 8


def test_executor_heap_leaves_overhead():
    # worker_pool 38 / 1 exec = 38 slice; heap = int(38 / 1.10) = 34; overhead = 34*1024*0.10
    r = resources_from_cell(_cell(executors_per_node=1, worker_mem_gb=38))
    assert r.executor_gb == 34
    assert r.executor_overhead_mb == int(34 * 1024 * 0.10)


def test_master_is_constant():
    r = resources_from_cell(_cell())
    assert r.master_gb == 1


def test_total_and_driver_carried_through():
    r = resources_from_cell(_cell(mem="48G", driver_mem_gb=4))
    assert r.total_gb == 48
    assert r.driver_gb == 4


def test_leftover_counts_both_daemon_jvms():
    # On the head node the real footprint is executor pool + driver + Master + the Worker
    # DAEMON's own JVM. The daemon used to be omitted, so `leftover_gb` over-reported the
    # free RAM by 1 GB per node and the head node could run the --mem cgroup dry.
    r = resources_from_cell(_cell(mem="48G", driver_mem_gb=4, worker_mem_gb=38))
    assert r.daemon_gb == 2
    assert r.leftover_gb == 48 - r.daemon_gb - 4 - 38


def test_returns_frozen_dataclass():
    r = resources_from_cell(_cell())
    assert isinstance(r, SparkResources)
    with pytest.raises(Exception):
        r.executor_cores = 99  # frozen
