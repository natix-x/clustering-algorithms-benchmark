#!/bin/bash -l
#SBATCH -J nyc-prep
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
# from the repo root: `sbatch sbatch_scripts/nyc_preprocessing.sbatch`.
REPO_ROOT="$SLURM_SUBMIT_DIR"
PY_SCRIPT="$REPO_ROOT/python/data_preprocessing/nyc_data/nyc_preprocessing.py"
export PYTHONPATH="$REPO_ROOT/python:${PYTHONPATH:-}"

if [ ! -f "$PY_SCRIPT" ]; then
    echo "ERROR: $PY_SCRIPT not found — did you run sbatch from the repo root?" >&2
    exit 1
fi

# Rows are small numeric records (no giant arrays like Cohere) -> no per-row-group OOM,
# so use all cores. Single-node local mode; spark-submit sets the master.
spark-submit \
  --master "local[${SLURM_CPUS_PER_TASK}]" \
  --driver-memory 150g \
  --conf spark.driver.maxResultSize=8g \
  "$PY_SCRIPT"
