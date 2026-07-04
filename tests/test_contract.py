"""Contract tests (Option A).

  * the schemas themselves are well-formed
  * each launcher's `build_run_config` output validates against
    run_config.schema.json — the invariant "the harness produces configs the
    engine jars can consume". Pure function, no cluster, no mocks.
  * recorded engine results validate against run_result.schema.json (drift guard)
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
RESULT_SCHEMA = json.loads((CONTRACT / "run_result.schema.json").read_text())

RESULT_FILES = (
    sorted((REPO / "local_testing" / "results").glob("*.json"))
    + sorted((REPO / "analysis" / "results").glob("*.json"))
)


def test_schemas_are_valid():
    jsonschema.Draft202012Validator.check_schema(CONFIG_SCHEMA)
    jsonschema.Draft202012Validator.check_schema(RESULT_SCHEMA)


@pytest.mark.parametrize("framework", ["spark", "flink"])
def test_build_run_config_matches_contract(framework, request):
    doc = request.getfixturevalue(f"{framework}_yaml")
    launcher = launchers.get_launcher(framework)
    validator = jsonschema.Draft202012Validator(CONFIG_SCHEMA)
    for experiment in generate_experiments(doc, 1):
        cfg = launcher.build_run_config(experiment, "/tmp/out", doc)
        validator.validate(cfg)


@pytest.mark.skipif(not RESULT_FILES, reason="no recorded result files")
@pytest.mark.parametrize("path", RESULT_FILES, ids=lambda p: p.name)
def test_result_validates(path):
    jsonschema.Draft202012Validator(RESULT_SCHEMA).validate(json.loads(path.read_text()))
