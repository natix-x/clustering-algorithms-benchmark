from __future__ import annotations

import logging
from dataclasses import dataclass

from slurm_experiments_orchestrator.common.yaml_validator import parse_mem_gb

logger = logging.getLogger(__name__)

#: Fraction of an executor's slice reserved as off-heap overhead (heap = slice/1.10,
#: overhead = heap * 0.10).
_EXECUTOR_OVERHEAD_FRACTION = 0.10

_MASTER_MEM_GB = 1


@dataclass(frozen=True)
class SparkResources:
    total_gb:             int
    master_gb:            int
    driver_gb:            int
    worker_pool_gb:       int
    executor_gb:          int
    executor_cores:       int
    executor_overhead_mb: int
    executors_per_node:   int

    @property
    def leftover_gb(self) -> int:
        """RAM left for OS/page cache after the explicit grants (informational)."""
        return self.total_gb - self.master_gb - self.driver_gb - self.worker_pool_gb

    def print_summary(self) -> None:
        logger.info(
            f"--- Spark resources: --mem={self.total_gb}G, "
            f"executors_per_node={self.executors_per_node} ---"
        )
        logger.info(f"  Spark Master:       {self.master_gb} GB")
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

    Granted (choices): mem, driver, master, worker pool. Derived (forced by the
    packing — standalone puts `executors_per_node` executors on the worker, splitting
    both its cores and its memory pool):
        executor_cores = cpus_per_task // executors_per_node
        executor_heap  = (worker_pool / executors_per_node) / 1.10
        overhead       = executor_heap * 0.10
    """
    cpus           = int(res["cpus_per_task"])
    execs          = int(res["executors_per_node"])
    worker_pool_gb = int(res["worker_mem_gb"])

    executor_cores = max(1, cpus // execs)
    if cpus % execs != 0:
        logger.warning(
            f"cpus_per_task={cpus} not divisible by executors_per_node={execs}; "
            f"{cpus - executor_cores * execs} core(s) will be idle."
        )

    slice_gb             = worker_pool_gb / execs
    executor_gb          = max(1, int(slice_gb / (1 + _EXECUTOR_OVERHEAD_FRACTION)))
    executor_overhead_mb = int(executor_gb * 1024 * _EXECUTOR_OVERHEAD_FRACTION)

    return SparkResources(
        total_gb=parse_mem_gb(res["mem"]),
        master_gb=_MASTER_MEM_GB,
        driver_gb=int(res["driver_mem_gb"]),
        worker_pool_gb=worker_pool_gb,
        executor_gb=executor_gb,
        executor_cores=executor_cores,
        executor_overhead_mb=executor_overhead_mb,
        executors_per_node=execs,
    )
