"""Spark resource packing math (cores/memory split across executors)."""
from __future__ import annotations

import logging

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


def test_cores_split_evenly():
    r = resources_from_cell(_cell(cpus_per_task=8, executors_per_node=2))
    assert r.executor_cores == 4  # 8 // 2


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


def test_leftover_is_total_minus_grants():
    r = resources_from_cell(_cell(mem="48G", driver_mem_gb=4, worker_mem_gb=38))
    assert r.leftover_gb == 48 - r.master_gb - 4 - 38


def test_non_divisible_cores_warns(caplog):
    with caplog.at_level(logging.WARNING):
        r = resources_from_cell(_cell(cpus_per_task=7, executors_per_node=2))
    assert r.executor_cores == 3  # 7 // 2
    assert "not divisible" in caplog.text


def test_returns_frozen_dataclass():
    r = resources_from_cell(_cell())
    assert isinstance(r, SparkResources)
    with pytest.raises(Exception):
        r.executor_cores = 99  # frozen
