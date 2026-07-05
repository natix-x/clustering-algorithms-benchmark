"""JobWriter (Strategy Context): writes per-run config JSONs + sbatch scripts."""
from __future__ import annotations

import json
import os
import stat

from slurm_experiments_orchestrator import launchers
from slurm_experiments_orchestrator.common.experiments_generator import generate_experiments
from slurm_experiments_orchestrator.slurm.jobs_writer import JobWriter


def _writer(framework):
    return JobWriter(launchers.get_launcher(framework))


def test_write_configs_creates_one_json_per_experiment(spark_yaml, tmp_path):
    spark_yaml["experiment_matrix"]["nodes"] = [1, 2]
    exps = generate_experiments(spark_yaml)
    _writer("spark").write_configs(exps, str(tmp_path), str(tmp_path), spark_yaml)

    files = sorted(tmp_path.glob("*.json"))
    assert len(files) == len(exps)
    for exp in exps:
        assert (tmp_path / f"{exp.run_id}.json").exists()


def test_written_config_is_valid_json_with_runid(spark_yaml, tmp_path):
    exp = generate_experiments(spark_yaml)[0]
    _writer("spark").write_configs([exp], str(tmp_path), str(tmp_path), spark_yaml)
    cfg = json.loads((tmp_path / f"{exp.run_id}.json").read_text())
    assert cfg["runId"] == exp.run_id


def test_write_sbatch_files_creates_scripts_and_submit_all(spark_yaml, tmp_path):
    exps = generate_experiments(spark_yaml)
    cfg_dir = tmp_path / "configs"
    cfg_dir.mkdir()
    sbatch_dir = tmp_path / "sbatch"
    _writer("spark").write_sbatch_files(
        exps, str(cfg_dir), str(tmp_path / "logs"), str(tmp_path / "out"),
        str(sbatch_dir), spark_yaml,
    )
    scripts = sorted(sbatch_dir.glob("*.sbatch"))
    assert len(scripts) == len(exps)
    assert (sbatch_dir / "submit_all.sh").exists()


def test_sbatch_scripts_are_executable(spark_yaml, tmp_path):
    exp = generate_experiments(spark_yaml)[0]
    sbatch_dir = tmp_path / "sbatch"
    _writer("spark").write_sbatch_files(
        [exp], str(tmp_path), str(tmp_path), str(tmp_path), str(sbatch_dir), spark_yaml,
    )
    script = sbatch_dir / f"{exp.run_id}.sbatch"
    assert script.stat().st_mode & stat.S_IXUSR


def test_submit_all_references_every_script(spark_yaml, tmp_path):
    exps = generate_experiments(spark_yaml)
    sbatch_dir = tmp_path / "sbatch"
    _writer("spark").write_sbatch_files(
        exps, str(tmp_path), str(tmp_path), str(tmp_path), str(sbatch_dir), spark_yaml,
    )
    submit_all = (sbatch_dir / "submit_all.sh").read_text()
    for exp in exps:
        assert f"{exp.run_id}.sbatch" in submit_all
