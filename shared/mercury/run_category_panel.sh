#!/bin/bash
#SBATCH --job-name=cat-panel
#SBATCH --account=phd
#SBATCH --partition=highmem
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=64G
#SBATCH --time=12:00:00
#SBATCH --output=%x_%a_slurm_%A.out
#SBATCH --error=%x_%a_slurm_%A.err

# Per-year parallel extraction. One SLURM array task per
# year; task IDs are small indices that map to years via
# YEAR_BASE + SLURM_ARRAY_TASK_ID. Mercury caps array IDs
# (cannot use 2010..2023 directly).
#
# Parallel usage (14 years × independent file scans):
#   sbatch --array=0-13 \
#       --export=CATEGORY=SSB \
#       taxes_ads/shared/mercury/run_category_panel.sh
#   → task 0 = 2010, task 13 = 2023
#
# Single-year debug (2017 = idx 7):
#   sbatch --array=7 \
#       --export=CATEGORY=SSB \
#       taxes_ads/shared/mercury/run_category_panel.sh
#
# Serial fallback (one task, many years in a loop):
#   sbatch --export=CATEGORY=SSB,YEARS=2010-2023 \
#       taxes_ads/shared/mercury/run_category_panel.sh

set -euo pipefail
source ~/venv/bin/activate
PYTHON_BIN="${PYTHON_BIN:-python3}"
CATEGORY="${CATEGORY:?must set CATEGORY=SSB|LIQUOR|...}"
YEAR_BASE="${YEAR_BASE:-2010}"

# If running as an array job, map task ID → year.
# Otherwise fall back to the YEARS env var.
if [[ -n "${SLURM_ARRAY_TASK_ID:-}" ]]; then
    YEARS="$((YEAR_BASE + SLURM_ARRAY_TASK_ID))"
else
    YEARS="${YEARS:-2010-2023}"
fi

SUBMIT_DIR="${SLURM_SUBMIT_DIR:-$PWD}"
SCRIPT_PATH="${SUBMIT_DIR}/taxes_ads/shared/mercury/extract_category_panel.py"

echo "=============================================="
echo "Category panel EXTRACTION"
echo "Category:    ${CATEGORY}"
echo "Years:       ${YEARS}"
echo "Job ID:      ${SLURM_JOB_ID:-local}"
echo "Array task:  ${SLURM_ARRAY_TASK_ID:-none}"
echo "Submit:      ${SUBMIT_DIR}"
echo "Script:      ${SCRIPT_PATH}"
echo "Start:       $(date)"
echo "=============================================="

"$PYTHON_BIN" "${SCRIPT_PATH}" \
    --category "${CATEGORY}" --years "${YEARS}"

echo "=============================================="
echo "Finished: $(date)"
ls -lh "${CATEGORY,,}_dma_month_panel_"*.csv 2>/dev/null || true
echo "=============================================="
