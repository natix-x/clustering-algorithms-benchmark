#!/bin/bash
# Submit a dataset preprocessing job on the generic preprocess.sbatch.
#
#   ./sbatch_scripts/run.sh <dataset>       # dataset: gaia | nyc | monet | tech_news
#
# Looks up the script path, walltime and Spark local-cores for the dataset, then submits
# preprocess.sbatch with those. Run from anywhere — it cd's to the repo root so
# $SLURM_SUBMIT_DIR (used to resolve paths in the sbatch) is the repo root.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."  # repo root

declare -A PY=(
    [gaia]=python/data_preprocessing/gaia_data/gaia_preprocessing.py
    [nyc]=python/data_preprocessing/nyc_data/nyc_preprocessing.py
    [monet]=python/data_preprocessing/monet_data/monet_preprocessing.py
    [tech_news]=python/data_preprocessing/tech_news_data/tech_news_processing.py
)
declare -A WALLTIME=(
    [gaia]=12:00:00
    [nyc]=04:00:00
    [monet]=02:00:00
    [tech_news]=02:00:00
)
# Spark local[N] cores; default 48. Add a per-dataset override here if a dataset needs fewer
# (e.g. wide embedding row groups OOM'ing the heap).
declare -A CORES=()

ds="${1:-}"
if [ -z "${PY[$ds]:-}" ]; then
    echo "usage: $0 {${!PY[*]}}" >&2
    exit 1
fi

cores="${CORES[$ds]:-48}"
set -x
sbatch -J "${ds}-prep" --time="${WALLTIME[$ds]}" \
    --export=ALL,PY="${PY[$ds]}",LOCAL_CORES="$cores" \
    sbatch_scripts/preprocess.sbatch
