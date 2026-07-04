"""Matrix expansion: cartesian product, run_id shape, repetitions, determinism."""
from __future__ import annotations

import re

from slurm_experiments_orchestrator.common.experiments_generator import (
    Experiment,
    generate_experiments,
)


def test_single_cell_one_experiment(spark_yaml):
    exps = generate_experiments(spark_yaml, 1)
    assert len(exps) == 1
    assert isinstance(exps[0], Experiment)


def test_cartesian_product_count(spark_yaml):
    # 2 nodes x 2 algorithms x 1 dataset x 1 resources = 4 cells
    spark_yaml["matrix"]["nodes"] = [1, 2]
    spark_yaml["matrix"]["algorithm"] = [
        {"name": "kmeans", "params": {"k": 3}},
        {"name": "dbscan", "params": {"eps": 0.5}},
    ]
    exps = generate_experiments(spark_yaml, 1)
    assert len(exps) == 4


def test_repetitions_multiply(spark_yaml):
    exps = generate_experiments(spark_yaml, 3)
    assert len(exps) == 3
    assert {e.rep for e in exps} == {0, 1, 2}


def test_run_id_shape_and_uniqueness(spark_yaml):
    spark_yaml["matrix"]["nodes"] = [1, 2]
    exps = generate_experiments(spark_yaml, 1)
    ids = [e.run_id for e in exps]
    assert len(set(ids)) == len(ids)  # unique
    for e in exps:
        assert re.match(rf"^t-nodes{e.nodes}-\d{{4}}-[0-9a-f]+$", e.run_id), e.run_id


def test_hash_is_deterministic(spark_yaml):
    a = generate_experiments(spark_yaml, 1)[0]
    b = generate_experiments(spark_yaml, 1)[0]
    assert a.run_id == b.run_id


def test_cell_carries_axis_values(spark_yaml):
    exp = generate_experiments(spark_yaml, 1)[0]
    assert exp.nodes == 1
    assert exp.cell["algorithm"]["name"] == "kmeans"
    assert exp.cell["dataset"]["type"] == "synthetic"
    assert "resources" in exp.cell
