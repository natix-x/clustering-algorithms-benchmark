#!/bin/bash       # generate only
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"
PYTHONPATH=python exec python3 -m slurm_experiments_orchestrator.run_experiments "$@"
