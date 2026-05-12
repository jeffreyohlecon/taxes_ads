#!/bin/bash
#SBATCH --job-name=philly-soda-discover
#SBATCH --account=phd
#SBATCH --partition=highmem
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=16G
#SBATCH --time=00:30:00
#SBATCH --output=%x_slurm_%j.out
#SBATCH --error=%x_slurm_%j.err

set -euo pipefail
source ~/venv/bin/activate
PYTHON_BIN="${PYTHON_BIN:-python3}"

# Submit from ~ ; script lives in repo subtree under home.
SUBMIT_DIR="${SLURM_SUBMIT_DIR:-$PWD}"
SCRIPT_PATH="${SUBMIT_DIR}/taxes_ads/experiments/philly_soda_tax/mercury/discover_soda_brands.py"

echo "=============================================="
echo "Philly Beverage Tax ad-spend DISCOVERY"
echo "Job ID: ${SLURM_JOB_ID:-local}"
echo "Submit dir: ${SUBMIT_DIR}"
echo "Script:     ${SCRIPT_PATH}"
echo "Start: $(date)"
echo "=============================================="

"$PYTHON_BIN" "${SCRIPT_PATH}"

echo "=============================================="
echo "Finished: $(date)"
echo "Outputs in: ${SUBMIT_DIR}"
ls -lh adintel_year_inventory.txt \
       soda_brand_candidates.csv \
       philadelphia_market_code.txt \
       spot_tv_head_*.tsv 2>/dev/null || true
echo "=============================================="
