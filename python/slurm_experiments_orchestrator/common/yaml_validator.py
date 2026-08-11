from __future__ import annotations

from utils.logger import get_logger
import re

from slurm_experiments_orchestrator.common.config import (
    MEM_STRING_KEYS,
    POSITIVE_DATASET_PARAMS,
    REQUIRED_ALGORITHM_KEYS,
    REQUIRED_DATASET_KEYS,
    REQUIRED_DATASET_PARAMS,
    REQUIRED_KEYS,
    REQUIRED_MATRIX_KEYS,
)

logger = get_logger(__name__)


def parse_mem_gb(mem_str: str) -> int:
    m = re.match(r'^(\d+)\s*[Gg]?$', str(mem_str).strip())
    if not m:
        raise ValueError(
            f"Wrong memory format: {mem_str!r}. Use e.g. '48G'."
        )
    return int(m.group(1))


def validate_yaml_config_file(
        parsed_yaml_config_file: dict,
        resource_keys: tuple[str, ...],
) -> None:
    missing = [k for k in REQUIRED_KEYS if k not in parsed_yaml_config_file]
    if missing:
        raise KeyError(f"Missing top-level keys in YAML: {missing}")

    matrix = parsed_yaml_config_file["experiment_matrix"]

    missing_matrix = [k for k in REQUIRED_MATRIX_KEYS if k not in matrix]
    if missing_matrix:
        raise KeyError(
            f"Missing required keys in 'experiment_matrix': {missing_matrix}. "
            f"Required: {list(REQUIRED_MATRIX_KEYS)}"
        )

    if "repetitions" in matrix:
        raise ValueError(
            "Repetitions should be top-level key in YAML, not in 'experiment_matrix'."
        )

    for key, values in matrix.items():
        if not isinstance(values, list) or len(values) == 0:
            raise ValueError(
                f"experiment_matrix['{key}'] must be a non-empty list, "
                f"got: {type(values).__name__} = {values!r}"
            )

    repetitions = parsed_yaml_config_file.get("repetitions")
    if not isinstance(repetitions, int) or repetitions < 1:
        raise ValueError("Repetitions must be a positive integer.")

    _validate_resources(parsed_yaml_config_file, resource_keys)
    _validate_matrix_entries(matrix["datasets"], "datasets", REQUIRED_DATASET_KEYS)
    _validate_matrix_entries(matrix["algorithms"], "algorithms", REQUIRED_ALGORITHM_KEYS)
    _validate_dataset_params(matrix["datasets"])
    logger.debug("YAML config passed validation.")


def _validate_dataset_params(datasets: list) -> None:
    for i, entry in enumerate(datasets):
        source = f"experiment_matrix.datasets[{i}]"
        params = entry["params"]

        missing = [k for k in REQUIRED_DATASET_PARAMS.get(entry["type"], ()) if k not in params]
        if missing:
            raise KeyError(f"{source}.params missing keys for type '{entry['type']}': {missing}")

        for key in POSITIVE_DATASET_PARAMS:
            value = params.get(key)
            if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value < 1):
                raise ValueError(f"{source}.params.{key} must be a positive integer, got {value!r}")

        fraction = params.get("sampleFraction")
        if fraction is not None and not (0 < float(fraction) <= 1):
            raise ValueError(f"{source}.params.sampleFraction must be in (0, 1], got {fraction!r}")


def _validate_matrix_entries(entries: list, axis: str, required_keys: tuple[str, ...]) -> None:
    """Each experiment_matrix.<axis> entry must be a dict defining all `required_keys`, with a
    dict `params`. Caught here (input) instead of later at run-config schema time."""
    for i, entry in enumerate(entries):
        if not isinstance(entry, dict):
            raise ValueError(
                f"experiment_matrix.{axis}[{i}] must be a dict, got {type(entry).__name__}"
            )
        missing = [k for k in required_keys if k not in entry]
        if missing:
            raise KeyError(
                f"experiment_matrix.{axis}[{i}] missing keys: {missing}. "
                f"Each entry must define: {list(required_keys)}"
            )
        if not isinstance(entry["params"], dict):
            raise ValueError(
                f"experiment_matrix.{axis}[{i}].params must be a dict, "
                f"got {type(entry['params']).__name__}"
            )


def _validate_resources(parsed_yaml_config_file: dict, resource_keys: tuple[str, ...]) -> None:
    """`experiment_matrix.resources` must exist and every entry must define all `resource_keys`."""
    matrix = parsed_yaml_config_file["experiment_matrix"]

    if "resources" not in matrix:
        raise KeyError("experiment_matrix.resources is required.")

    for i, entry in enumerate(matrix["resources"]):
        if not isinstance(entry, dict):
            raise ValueError(
                f"experiment_matrix.resources[{i}] must be a dict, got {type(entry).__name__}"
            )
        missing = [k for k in resource_keys if k not in entry]
        if missing:
            raise KeyError(
                f"experiment_matrix.resources[{i}] missing keys: {missing}. "
                f"Each entry must explicitly define: {list(resource_keys)}"
            )
        _validate_resource_entry(entry, f"experiment_matrix.resources[{i}]", resource_keys)


def _validate_resource_entry(entry: dict, source: str, resource_keys: tuple[str, ...]) -> None:
    for key in resource_keys:
        value = entry[key]
        if key in MEM_STRING_KEYS:
            parse_mem_gb(value)
            continue
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ValueError(
                f"{source}.{key} must be a positive integer, got {value!r}"
            )
