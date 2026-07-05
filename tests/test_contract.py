"""Contract tests (Option A).

  * the schemas themselves are well-formed
  * each launcher's `build_run_config` output validates against
    run_config.schema.json — the invariant "the harness produces configs the
    engine jars can consume". Pure function, no cluster, no mocks.
"""
from __future__ import annotations

import json
from pathlib import Path

import jsonschema
import pytest

from slurm_experiments_orchestrator import launchers
from slurm_experiments_orchestrator.common.experiments_generator import generate_experiments

REPO = Path(__file__).resolve().parents[1]
CONTRACT = REPO / "contract"

CONFIG_SCHEMA = json.loads((CONTRACT / "run_config.schema.json").read_text())


def test_config_schema_is_valid():
    jsonschema.Draft202012Validator.check_schema(CONFIG_SCHEMA)


@pytest.mark.parametrize("framework", ["spark", "flink"])
def test_build_run_config_matches_contract(framework, request):
    yaml_config = request.getfixturevalue(f"{framework}_yaml")
    launcher = launchers.get_launcher(framework)
    validator = jsonschema.Draft202012Validator(CONFIG_SCHEMA)
    for experiment in generate_experiments(yaml_config):
        cfg = launcher.build_run_config(experiment, "/tmp/out", yaml_config)
        validator.validate(cfg)
