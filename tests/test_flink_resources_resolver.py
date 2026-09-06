from __future__ import annotations

import pytest

from slurm_experiments_orchestrator.launchers.flink_resources_resolver import (
    FlinkResources,
    resources_from_cell,
)
from slurm_experiments_orchestrator.launchers.spark_resources_resolver import (
    resources_from_cell as spark_resources_from_cell,
)


def _cell(**over):
    base = {"cpus_per_task": 8, "tm_per_node": 2, "mem": "48G",
            "tm_mem_gb": 38, "jm_mem_gb": 5}
    base.update(over)
    return base


def _spark_twin(**over):
    """The Spark cell meaning the same thing: same cores, same pool, same node."""
    base = {"cpus_per_task": 8, "executors_per_node": 2, "mem": "48G",
            "driver_mem_gb": 4, "worker_mem_gb": 38}
    base.update(over)
    return base


def _flink_split(process_mb: int, network_cap_mb: int) -> tuple[int, int]:
    """Flink 1.17's published TaskManager derivation -> (managed, task heap), in MB.

    Total Flink Memory = process - metaspace - JVM overhead; managed is 0.4 of it, network
    0.1 of it (capped), and the task heap is what remains after the framework's own 128 MB
    heap and 128 MB off-heap.
    """
    overhead_mb  = max(192, min(1024, int(0.10 * process_mb)))
    flink_mb     = process_mb - 256 - overhead_mb
    managed_mb   = int(0.40 * flink_mb)
    network_mb   = min(int(0.10 * flink_mb), network_cap_mb)
    task_heap_mb = flink_mb - managed_mb - network_mb - 128 - 128
    return managed_mb, task_heap_mb


# --- the parity property that lets the resolver stay thin ---

@pytest.mark.parametrize("cpus,per_node,pool,mem", [
    (4, 1, 41, "48G"),
    (8, 1, 76, "96G"),
    (8, 2, 76, "96G"),
    (8, 4, 76, "96G"),
    (32, 1, 76, "96G"),
    (4, 1, 18, "24G"),
])
def test_flinks_own_split_already_matches_the_spark_executor_heap(cpus, per_node, pool, mem):
    # Spark's one heap holds user objects AND the cached input; Flink puts the cache in MANAGED
    # memory. Their sum is the comparable quantity, and two unrelated default formulas land it
    # within 10% — which is what makes passing the pool through to Flink untouched defensible.
    f = resources_from_cell(_cell(cpus_per_task=cpus, tm_per_node=per_node,
                                  tm_mem_gb=pool, mem=mem))
    s = spark_resources_from_cell(_spark_twin(cpus_per_task=cpus, executors_per_node=per_node,
                                              worker_mem_gb=pool, mem=mem))
    managed_mb, task_heap_mb = _flink_split(f.tm_process_mb, f.tm_network_max_mb)
    ratio = (s.executor_gb * 1024) / (managed_mb + task_heap_mb)
    assert 0.9 <= ratio <= 1.1, f"user-memory ratio {ratio:.3f} outside 10%"


def test_task_heap_alone_would_look_unfair():
    # Guards the reasoning above. If this stops holding, the sum-vs-heap distinction has gone
    # away and the parity test above has quietly become vacuous.
    f = resources_from_cell(_cell(tm_per_node=1, tm_mem_gb=41, cpus_per_task=4, mem="48G"))
    s = spark_resources_from_cell(_spark_twin(executors_per_node=1, worker_mem_gb=41,
                                              cpus_per_task=4, mem="48G"))
    managed_mb, task_heap_mb = _flink_split(f.tm_process_mb, f.tm_network_max_mb)
    assert (s.executor_gb * 1024) / task_heap_mb > 1.4
    assert managed_mb > 0


# --- what the resolver actually does ---

def test_pool_reaches_the_taskmanager_unsplit():
    f = resources_from_cell(_cell(tm_mem_gb=38, tm_per_node=2))
    assert f.tm_process_mb == 38 * 1024 // 2


def test_jobmanager_heap_matches_the_spark_driver_heap():
    # Under Application Mode main() runs INSIDE the JobManager, so the JM is the driver and its
    # HEAP — not its process total — is what pairs with --driver-memory. `jm_mem_gb: 1` against
    # `driver_mem_gb: 4` used to mean 448 MB against 4096 MB.
    f = resources_from_cell(_cell(jm_mem_gb=5))
    s = spark_resources_from_cell(_spark_twin(driver_mem_gb=4))
    assert abs(f.jm_heap_mb - s.driver_gb * 1024) < 512


def test_undersized_jobmanager_is_refused():
    with pytest.raises(ValueError, match="JobManager"):
        resources_from_cell(_cell(jm_mem_gb=1))


def test_undersized_taskmanager_is_refused_at_submit_time():
    # Flink would reject it too, but only once the cluster boots — after the job has queued.
    with pytest.raises(ValueError, match="TaskManager"):
        resources_from_cell(_cell(tm_mem_gb=2, tm_per_node=2))


def test_network_cap_doubles_flinks_own():
    assert resources_from_cell(_cell()).tm_network_max_mb == 2048


def test_returns_frozen_dataclass():
    f = resources_from_cell(_cell())
    assert isinstance(f, FlinkResources)
    with pytest.raises(Exception):
        f.tm_process_mb = 99
