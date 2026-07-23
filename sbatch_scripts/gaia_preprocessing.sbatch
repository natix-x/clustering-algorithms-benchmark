#!/bin/bash -l
#SBATCH -J gaia-prep
#SBATCH -N 1
#SBATCH --cpus-per-task=48
#SBATCH --mem=180G
#SBATCH --exclusive
#SBATCH --time=12:00:00
#SBATCH -A plgclusteringbigdata-cpu
#SBATCH -p plgrid
#SBATCH --output=%x_%j.out
#SBATCH --error=%x_%j.err

set -euo pipefail
module purge && module load spark/3.3.2-hadoop-3.2-java-11

# SLURM copies the batch script to a spool dir, so $0/BASH_SOURCE point there — useless
# for locating repo files. Use $SLURM_SUBMIT_DIR (the dir you ran `sbatch` from). Submit
# from the repo root: `sbatch sbatch_scripts/gaia_preprocessing.sbatch`.
REPO_ROOT="$SLURM_SUBMIT_DIR"
PY_SCRIPT="$REPO_ROOT/python/data_preprocessing/gaia_data/gaia_preprocessing.py"
export PYTHONPATH="$REPO_ROOT/python:${PYTHONPATH:-}"

if [ ! -f "$PY_SCRIPT" ]; then
    echo "ERROR: $PY_SCRIPT not found — did you run sbatch from the repo root?" >&2
    exit 1
fi

# Small numeric rows (no giant arrays), so all cores are safe. ~681 GB read TWICE (stats
# + write) in single-node local mode -> IO-bound, hence the long walltime. spark-submit
# sets the master.
spark-submit \
  --master "local[${SLURM_CPUS_PER_TASK}]" \
  --driver-memory 150g \
  --conf spark.driver.maxResultSize=8g \
  "$PY_SCRIPT"
