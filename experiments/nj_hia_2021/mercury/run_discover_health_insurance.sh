#!/bin/bash
#SBATCH --job-name=hi_discover
#SBATCH --account=phd
#SBATCH --partition=highmem
#SBATCH --mem=32G
#SBATCH --time=00:30:00
#SBATCH --output=hi_discover_%j.out
#SBATCH --error=hi_discover_%j.err

set -euo pipefail

source "${HOME}/venv/bin/activate"

SCRIPT="${SLURM_SUBMIT_DIR}/taxes_ads/experiments/nj_hia_2021/mercury/discover_health_insurance.py"

echo "Host: $(hostname)"
echo "Submit dir: ${SLURM_SUBMIT_DIR}"
echo "Script: ${SCRIPT}"
echo "Python: $(which python3)"
date

python3 "${SCRIPT}"

date
echo "Done."
