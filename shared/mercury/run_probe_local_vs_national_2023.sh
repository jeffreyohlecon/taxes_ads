#!/bin/bash
#SBATCH --job-name=probe-locnatl-2023
#SBATCH --account=phd
#SBATCH --partition=highmem
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=64G
#SBATCH --time=04:00:00
#SBATCH --output=%x_slurm_%j.out
#SBATCH --error=%x_slurm_%j.err

# One-shot probe: SSB + BEER spend by parent × source
# media type in 2023. Six TV media folders, ~17 GB of
# TSV total. Expected wall ~30 min.
#
# Submit from home:
#   sbatch taxes_ads/shared/mercury/run_probe_local_vs_national_2023.sh
# Output:
#   probe_local_vs_national_2023.csv in submit dir

set -euo pipefail
source ~/venv/bin/activate
PYTHON_BIN="${PYTHON_BIN:-python3}"

SUBMIT_DIR="${SLURM_SUBMIT_DIR:-$PWD}"
SCRIPT_PATH="${SUBMIT_DIR}/taxes_ads/shared/mercury/probe_local_vs_national_2023.py"

echo "=============================================="
echo "Probe: local-vs-national TV, 2023"
echo "Job ID:  ${SLURM_JOB_ID:-local}"
echo "Submit:  ${SUBMIT_DIR}"
echo "Script:  ${SCRIPT_PATH}"
echo "Start:   $(date)"
echo "=============================================="

"$PYTHON_BIN" "${SCRIPT_PATH}"

echo "=============================================="
echo "Finished: $(date)"
ls -lh probe_local_vs_national_2023.csv 2>/dev/null || true
echo "=============================================="
