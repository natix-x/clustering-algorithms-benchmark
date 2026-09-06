"""
Flink resource resolver — deliberately thin.

Unlike Spark, Flink derives its own task heap, managed memory, and network buffers
from a single `taskmanager.memory.process.size`. We leave this derivation to Flink
(relying on proper Managed Memory utilization at the operator level).

This module pins exactly two critical overrides:
  1. JobManager heap: Historically sized to match Spark's driver in Application Mode.
     Under Session Mode (where the client is the driver), this budget is now generous
     rather than load-bearing, but kept identical to maintain a consistent control variable.
  2. Network buffer cap: Lifted to 2048 MB to prevent "Insufficient number of network
     buffers" OOMs at high parallelisms.
"""

from __future__ import annotations

from dataclasses import dataclass

from slurm_experiments_orchestrator.common.yaml_validator import parse_mem_gb
from utils.logger import get_logger

logger = get_logger(__name__)

# Network cap: Double Flink's 1GB default. Prevents OOMs at high parallelism
# without scaling infinitely with TM process size (which would steal from task heap).
_NETWORK_CAP_MB = 2048

# Reject undersized TMs at submit time rather than waiting for Flink to fail on the cluster.
_TM_MIN_PROCESS_MB = 2048

# Flink's JobManager process-to-heap deduction constants
_JM_METASPACE_MB    = 256
_JM_OFF_HEAP_MB     = 128
_JM_OVERHEAD_RATIO  = 0.10
_JM_OVERHEAD_MIN_MB = 192
_JM_OVERHEAD_MAX_MB = 1024

# JobManager heap floor.
_JM_MIN_HEAP_MB = 1024


@dataclass(frozen=True)
class FlinkResources:
    total_gb: int
    tms_per_node: int
    slots_per_tm: int
    tm_process_mb: int
    tm_network_max_mb: int
    jm_process_mb: int
    jm_heap_mb: int

    @property
    def leftover_gb(self) -> int:
        """RAM left for OS/page cache after the explicit grants (informational)."""
        granted_mb = self.tms_per_node * self.tm_process_mb + self.jm_process_mb
        return self.total_gb - (granted_mb // 1024)

    def print_summary(self) -> None:
        logger.info(
            f"--- Flink resources: --mem={self.total_gb}G, "
            f"tm_per_node={self.tms_per_node} ---"
        )
        logger.info(f"  JobManager:         {self.jm_process_mb} MB process, "
                    f"{self.jm_heap_mb} MB heap (historically paired with Spark driver)")
        logger.info(f"  TaskManager:        {self.tm_process_mb} MB process "
                    f"({self.tms_per_node}/node), split by Flink")
        logger.info(f"    network cap:      {self.tm_network_max_mb} MB (Flink default: 1024)")
        logger.info(f"  Slots/TaskManager:  {self.slots_per_tm}")
        logger.info(f"  Leftover (OS/cache):{self.leftover_gb} GB")


def jobmanager_heap_mb(jm_process_mb: int) -> int:
    """Calculates the heap Flink will allocate out of a given process total."""
    overhead_mb = max(_JM_OVERHEAD_MIN_MB, min(_JM_OVERHEAD_MAX_MB, int(_JM_OVERHEAD_RATIO * jm_process_mb)))
    return jm_process_mb - overhead_mb - _JM_METASPACE_MB - _JM_OFF_HEAP_MB


def resources_from_cell(res: dict) -> FlinkResources:
    """Build FlinkResources from an explicit `resources` entry."""
    tms = int(res["tm_per_node"])
    slots = int(res["cpus_per_task"])
    tm_pool_gb = int(res["tm_mem_gb"])
    jm_gb = int(res["jm_mem_gb"])

    tm_process_mb = (tm_pool_gb * 1024) // tms
    if tm_process_mb < _TM_MIN_PROCESS_MB:
        raise ValueError(
            f"tm_mem_gb={tm_pool_gb} with tm_per_node={tms} leaves {tm_process_mb} MB "
            f"per TaskManager. Minimum required by Flink is {_TM_MIN_PROCESS_MB} MB. "
            f"Raise tm_mem_gb or lower tm_per_node."
        )

    jm_process_mb = jm_gb * 1024
    jm_heap_mb = jobmanager_heap_mb(jm_process_mb)

    if jm_heap_mb < _JM_MIN_HEAP_MB:
        raise ValueError(
            f"jm_mem_gb={jm_gb} yields a JobManager heap of {jm_heap_mb} MB, which is "
            f"below the {_JM_MIN_HEAP_MB} MB floor. Increase jm_mem_gb."
        )

    return FlinkResources(
        total_gb=parse_mem_gb(res["mem"]),
        tms_per_node=tms,
        slots_per_tm=slots,
        tm_process_mb=tm_process_mb,
        tm_network_max_mb=_NETWORK_CAP_MB,
        jm_process_mb=jm_process_mb,
        jm_heap_mb=jm_heap_mb,
    )
