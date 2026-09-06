"""
Spark resource resolver.

Calculates memory splits for Spark standalone mode. Unlike Flink, Spark standalone
requires explicit, manual allocation of memory for executors, driver, and daemon JVMs.
"""

from __future__ import annotations

from dataclasses import dataclass

from slurm_experiments_orchestrator.common.yaml_validator import parse_mem_gb
from utils.logger import get_logger

logger = get_logger(__name__)

# Executor off-heap overhead fraction (heap = slice/1.10, overhead = heap * 0.10)
_EXECUTOR_OVERHEAD_FRACTION = 0.10

# Daemon JVM heaps (exported to SPARK_DAEMON_MEMORY in the sbatch template).
# Worker daemon is an unbudgeted JVM on every node.
_MASTER_MEM_GB = 1
_WORKER_DAEMON_MEM_GB = 1


@dataclass(frozen=True)
class SparkResources:
    total_gb: int
    master_gb: int
    driver_gb: int
    worker_pool_gb: int
    executor_gb: int
    executor_cores: int
    executor_overhead_mb: int
    executors_per_node: int

    @property
    def daemon_gb(self) -> int:
        """Combined heap for the Master and its Worker daemon on the head node."""
        return self.master_gb + _WORKER_DAEMON_MEM_GB

    @property
    def leftover_gb(self) -> int:
        """RAM left for OS/page cache, accounting for explicit grants and BOTH daemon JVMs."""
        return self.total_gb - self.daemon_gb - self.driver_gb - self.worker_pool_gb

    def print_summary(self) -> None:
        logger.info(
            f"--- Spark resources: --mem={self.total_gb}G, "
            f"executors_per_node={self.executors_per_node} ---"
        )
        logger.info(f"  Spark daemons:      {self.daemon_gb} GB "
                    f"(Master {self.master_gb} + Worker daemon {_WORKER_DAEMON_MEM_GB})")
        logger.info(f"  Spark Driver:       {self.driver_gb} GB")
        logger.info(f"  Worker pool:        {self.worker_pool_gb} GB")
        logger.info(
            f"  Memory/executor:    {self.executor_gb} GB heap "
            f"+ {self.executor_overhead_mb} MB overhead "
            f"({self.executors_per_node}/node)"
        )
        logger.info(
            f"  Cores/executor:     {self.executor_cores} "
            f"({self.executors_per_node}/node)"
        )
        logger.info(f"  Leftover (OS/cache):{self.leftover_gb} GB")


def resources_from_cell(res: dict) -> SparkResources:
    """Build SparkResources from an explicit `resources` entry.

    `cpus_per_task` matches Flink's `slots_per_tm`, aligning total core counts.
    Standalone packs `executors_per_node` onto one Worker daemon, so the daemon 
    must advertise their combined core/memory pool.
    """
    cpus = int(res["cpus_per_task"])
    execs = int(res["executors_per_node"])
    worker_pool_gb = int(res["worker_mem_gb"])

    # slice = heap + (heap * 0.10)
    slice_gb = worker_pool_gb / execs
    executor_gb = max(1, int(slice_gb / (1 + _EXECUTOR_OVERHEAD_FRACTION)))
    executor_overhead_mb = int(executor_gb * 1024 * _EXECUTOR_OVERHEAD_FRACTION)

    return SparkResources(
        total_gb=parse_mem_gb(res["mem"]),
        master_gb=_MASTER_MEM_GB,
        driver_gb=int(res["driver_mem_gb"]),
        worker_pool_gb=worker_pool_gb,
        executor_gb=executor_gb,
        executor_cores=cpus,
        executor_overhead_mb=executor_overhead_mb,
        executors_per_node=execs,
    )
