#!/bin/bash -l
#SBATCH -J preprocess
#SBATCH -N 1
#SBATCH --cpus-per-task=48
#SBATCH --mem=180G
#SBATCH --exclusive
#SBATCH --time=04:00:00
#SBATCH -A plgclusteringbigdata-cpu
#SBATCH -p plgrid
#SBATCH --output=%x_%j.out
#SBATCH --error=%x_%j.err

# Generic single-node preprocessing job. The dataset-specific bits (which script, job
# name, walltime, local cores) are passed at submit time — see sbatch_scripts/run.sh,
# which fills them per dataset. Directives above are just defaults; sbatch CLI flags
# (-J, --time) and --export (PY, LOCAL_CORES) override them.
#
#   sbatch -J monet-prep --time=02:00:00 \
#     --export=ALL,PY=python/data_preprocessing/monet_data/monet_preprocessing.py \
#     sbatch_scripts/preprocess.sbatch

set -euo pipefail
module purge && module load spark/3.3.2-hadoop-3.2-java-11

# PY is a repo-root-relative path; SLURM copies the script to a spool dir, so resolve
# against $SLURM_SUBMIT_DIR (the dir you ran `sbatch` from — must be the repo root).
: "${PY:?set PY=python/data_preprocessing/<dataset>/<script>.py via --export}"
PY_SCRIPT="$SLURM_SUBMIT_DIR/$PY"
LOCAL_CORES="${LOCAL_CORES:-$SLURM_CPUS_PER_TASK}"
export PYTHONPATH="$SLURM_SUBMIT_DIR/python:${PYTHONPATH:-}"

if [ ! -f "$PY_SCRIPT" ]; then
    echo "ERROR: $PY_SCRIPT not found — did you run sbatch from the repo root?" >&2
    exit 1
fi

spark-submit \
  --master "local[${LOCAL_CORES}]" \
  --driver-memory 150g \
  --conf spark.driver.maxResultSize=8g \
  "$PY_SCRIPT"
