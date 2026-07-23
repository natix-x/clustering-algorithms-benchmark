#!/bin/bash -l
#SBATCH -J cohere-prep
#SBATCH -N 1
#SBATCH --cpus-per-task=48
#SBATCH --mem=180G
#SBATCH --exclusive
#SBATCH --time=04:00:00
#SBATCH -A plgclusteringbigdata-cpu
#SBATCH -p plgrid
#SBATCH --output=%x_%j.out
#SBATCH --error=%x_%j.err

set -euo pipefail
module purge && module load spark/3.3.2-hadoop-3.2-java-11

# SLURM copies the batch script to a spool dir, so $0/BASH_SOURCE point there — useless
# for locating repo files. Use $SLURM_SUBMIT_DIR (the dir you ran `sbatch` from). Submit
# from the repo root: `sbatch sbatch_scripts/cohere_preprocess.sbatch`.
REPO_ROOT="$SLURM_SUBMIT_DIR"
PY_SCRIPT="$REPO_ROOT/python/data_preprocessing/cohere_data/cohere_preprocessing.py"
export PYTHONPATH="$REPO_ROOT/python:${PYTHONPATH:-}"

if [ ! -f "$PY_SCRIPT" ]; then
    echo "ERROR: $PY_SCRIPT not found — did you run sbatch from the repo root?" >&2
    exit 1
fi

# local[N]: N = concurrent read tasks. `emb` is array<float> -> Spark falls back to the
# non-vectorized Parquet reader, which buffers a WHOLE row group per task. With big row
# groups, local[48] blew the heap (OOM). Cap concurrency so N x row_group_size fits in
# the 150g heap. Drop further (local[8]) if it still OOMs.
spark-submit \
  --master "local[16]" \
  --driver-memory 150g \
  --conf spark.driver.maxResultSize=8g \
  "$PY_SCRIPT"
