from slurm_experiments_orchestrator.launchers.base_launcher import Launcher
from slurm_experiments_orchestrator.launchers.flink_launcher import FlinkLauncher
from slurm_experiments_orchestrator.launchers.spark_launcher import SparkLauncher

_LAUNCHERS: dict[str, type[Launcher]] = {
    "spark": SparkLauncher,
    "flink": FlinkLauncher,
}


def get_launcher(name: str) -> Launcher:
    key = name.lower()
    if key not in _LAUNCHERS:
        raise ValueError(
            f"Unknown framework {name!r}. Available: {sorted(_LAUNCHERS)}"
        )
    return _LAUNCHERS[key]()


def available() -> list[str]:
    return sorted(_LAUNCHERS)
