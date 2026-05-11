#!/bin/bash
#SBATCH --job-name=philly-soda-extract
#SBATCH --account=phd
#SBATCH --partition=highmem
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=128G
#SBATCH --time=12:00:00
#SBATCH --output=%x_slurm_%j.out
#SBATCH --error=%x_slurm_%j.err

set -euo pipefail
source ~/venv/bin/activate
PYTHON_BIN="${PYTHON_BIN:-python3}"

SUBMIT_DIR="${SLURM_SUBMIT_DIR:-$PWD}"
SCRIPT_PATH="${SUBMIT_DIR}/taxes_ads/experiments/philly_soda_tax/mercury/extract_philly_spend.py"

echo "=============================================="
echo "Philly Beverage Tax ad-spend EXTRACTION"
echo "Job ID: ${SLURM_JOB_ID:-local}"
echo "Submit dir: ${SUBMIT_DIR}"
echo "Script:     ${SCRIPT_PATH}"
echo "Start: $(date)"
echo "=============================================="

# Expect ~soda_brand_candidates.csv to be in SUBMIT_DIR.
"$PYTHON_BIN" "${SCRIPT_PATH}"

echo "=============================================="
echo "Finished: $(date)"
ls -lh philly_soda_dma_spend_*.csv 2>/dev/null || true
echo "=============================================="
