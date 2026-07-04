"""Input validation: yaml_validator rejects malformed YAML configs, accepts valid ones."""
from __future__ import annotations

import pytest

from slurm_experiments_orchestrator.common.config import SPARK_RESOURCE_KEYS
from slurm_experiments_orchestrator.common.yaml_validator import (
    parse_mem_gb,
    validate_yaml_config_file,
)


def _validate(doc: dict) -> None:
    validate_yaml_config_file(doc, resource_keys=SPARK_RESOURCE_KEYS)


def test_valid_config_passes(spark_yaml):
    _validate(spark_yaml)  # must not raise


# --- top-level / matrix structure ---

@pytest.mark.parametrize("missing", ["name", "configs_dir", "log_dir", "output_dir", "jar_path", "experiment_matrix"])
def test_missing_top_level_key_raises(spark_yaml, missing):
    del spark_yaml[missing]
    with pytest.raises(KeyError):
        _validate(spark_yaml)


@pytest.mark.parametrize("missing", ["nodes", "algorithms", "datasets"])
def test_missing_matrix_key_raises(spark_yaml, missing):
    del spark_yaml["experiment_matrix"][missing]
    with pytest.raises(KeyError):
        _validate(spark_yaml)


def test_repetitions_inside_matrix_raises(spark_yaml):
    spark_yaml["experiment_matrix"]["repetitions"] = [2]
    with pytest.raises(ValueError):
        _validate(spark_yaml)


@pytest.mark.parametrize("bad", [[], "notalist", 5])
def test_matrix_axis_must_be_nonempty_list(spark_yaml, bad):
    spark_yaml["experiment_matrix"]["nodes"] = bad
    with pytest.raises(ValueError):
        _validate(spark_yaml)


@pytest.mark.parametrize("bad", [0, -1, "x", 1.5])
def test_repetitions_must_be_positive_int(spark_yaml, bad):
    spark_yaml["repetitions"] = bad
    with pytest.raises(ValueError):
        _validate(spark_yaml)


# --- resources ---

def test_missing_resource_key_raises(spark_yaml):
    del spark_yaml["experiment_matrix"]["resources"][0]["worker_mem_gb"]
    with pytest.raises(KeyError):
        _validate(spark_yaml)


def test_matrix_resources_required(spark_yaml):
    del spark_yaml["experiment_matrix"]["resources"]
    with pytest.raises(KeyError):
        _validate(spark_yaml)


@pytest.mark.parametrize("bad", [0, -4, "8", 4.0, True])
def test_resource_int_key_must_be_positive_int(spark_yaml, bad):
    spark_yaml["experiment_matrix"]["resources"][0]["cpus_per_task"] = bad
    with pytest.raises(ValueError):
        _validate(spark_yaml)


@pytest.mark.parametrize("mem", ["48G", "48g", "48", "7 G"])
def test_parse_mem_gb_accepts(mem):
    assert parse_mem_gb(mem) > 0


@pytest.mark.parametrize("mem", ["", "big", "4GB", "4.5G", "-4G"])
def test_parse_mem_gb_rejects(mem):
    with pytest.raises(ValueError):
        parse_mem_gb(mem)


# --- dataset / algorithm entry shape ---

@pytest.mark.parametrize("axis,drop", [("datasets", "type"), ("datasets", "params"),
                                       ("algorithms", "name"), ("algorithms", "params")])
def test_entry_missing_required_key_raises(spark_yaml, axis, drop):
    del spark_yaml["experiment_matrix"][axis][0][drop]
    with pytest.raises(KeyError):
        _validate(spark_yaml)


@pytest.mark.parametrize("axis", ["datasets", "algorithms"])
def test_entry_params_must_be_dict(spark_yaml, axis):
    spark_yaml["experiment_matrix"][axis][0]["params"] = "oops"
    with pytest.raises(ValueError):
        _validate(spark_yaml)


@pytest.mark.parametrize("axis", ["datasets", "algorithms"])
def test_entry_must_be_dict(spark_yaml, axis):
    spark_yaml["experiment_matrix"][axis][0] = "notadict"
    with pytest.raises(ValueError):
        _validate(spark_yaml)


def test_flink_config_valid(flink_yaml):
    from slurm_experiments_orchestrator.common.config import FLINK_RESOURCE_KEYS
    validate_yaml_config_file(flink_yaml, resource_keys=FLINK_RESOURCE_KEYS)
