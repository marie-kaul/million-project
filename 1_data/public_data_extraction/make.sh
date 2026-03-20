#!/bin/bash

# Trap to handle shell script errors
trap 'error_handler' ERR
error_handler() {
    error_time=$(date '+%Y-%m-%d %H:%M:%S')
    echo -e "\n\033[0;31mWarning\033[0m: make.sh failed at ${error_time}. Check above for details."
    exit 1
}

# Set paths
MAKE_SCRIPT_DIR="$(cd "$(dirname -- "$0")" && pwd -P)"
REPO_ROOT="$(git rev-parse --show-toplevel)"
MODULE=$(basename "$MAKE_SCRIPT_DIR")
LOGFILE="${MAKE_SCRIPT_DIR}/output/make.log"

# Check setup
source "${REPO_ROOT}/lib/shell/check_setup.sh"

# Tell user what we're doing
echo -e "\n\nMaking module \033[35m${MODULE}\033[0m with shell ${SHELL}"

# Load settings & tools
source "${REPO_ROOT}/local_env.sh"
source "${REPO_ROOT}/lib/shell/run_python.sh"

# Clear output directory (skip if RESUME=1 to allow partial re-runs)
if [[ "${RESUME:-0}" == "1" ]]; then
    echo -e "  \033[0;34mResume mode\033[0m: keeping existing output files (scripts will skip completed steps)"
    mkdir -p "${MAKE_SCRIPT_DIR}/output"
else
    rm -rf "${MAKE_SCRIPT_DIR}/output"
    mkdir -p "${MAKE_SCRIPT_DIR}/output"
fi

# Add symlink input files to local /input/ directory
(   cd "${MAKE_SCRIPT_DIR}"
    source "${MAKE_SCRIPT_DIR}/get_inputs.sh"
)

# Run scripts
echo -e "\nmake.sh started at $(date '+%Y-%m-%d %H:%M:%S')"

(
cd "${MAKE_SCRIPT_DIR}/source"

run_python download_scb_panel.py       "${LOGFILE}" || exit 1
run_python prepare_crime.py            "${LOGFILE}" || exit 1
) || false

echo -e "\nmake.sh finished at $(date '+%Y-%m-%d %H:%M:%S')" | tee -a "${LOGFILE}"
