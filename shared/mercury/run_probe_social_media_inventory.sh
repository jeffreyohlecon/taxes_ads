#!/bin/bash
#SBATCH --job-name=probe-social-inv
#SBATCH --account=phd
#SBATCH --partition=highmem
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=16G
#SBATCH --time=00:30:00
#SBATCH --output=%x_slurm_%j.out
#SBATCH --error=%x_slurm_%j.err

# Pure-discovery probe: inventory social media data under
# /kilts/adintel for 2022+. No parsing or aggregation; just
# walks the directory tree, prints file sizes, header rows,
# and 3 sample rows per .tsv. Writes
# social_media_inventory.txt in the submit dir.
#
# Submit from home:
#   sbatch taxes_ads/shared/mercury/run_probe_social_media_inventory.sh

set -euo pipefail
source ~/venv/bin/activate
PYTHON_BIN="${PYTHON_BIN:-python3}"

SUBMIT_DIR="${SLURM_SUBMIT_DIR:-$PWD}"
SCRIPT_PATH="${SUBMIT_DIR}/taxes_ads/shared/mercury/probe_social_media_inventory.py"

echo "=============================================="
echo "Probe: social media inventory"
echo "Job ID:  ${SLURM_JOB_ID:-local}"
echo "Submit:  ${SUBMIT_DIR}"
echo "Script:  ${SCRIPT_PATH}"
echo "Start:   $(date)"
echo "=============================================="

"$PYTHON_BIN" "${SCRIPT_PATH}"

echo "=============================================="
echo "Finished: $(date)"
ls -lh social_media_inventory.txt 2>/dev/null || true
echo "=============================================="
