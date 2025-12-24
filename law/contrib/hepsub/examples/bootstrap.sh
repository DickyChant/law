#!/bin/bash

# Bootstrap script for HEPSub jobs on IHEP cluster
# This script sets up the environment on the worker node before running the actual job
#
# NOTE: At IHEP, jobs run in the submission directory with access to the shared filesystem.
#       Files can be read and written directly without staging.

echo "=== HEPSub Job Bootstrap ==="
echo "Node: $(hostname)"
echo "User: $(whoami)"
echo "Start time: $(date)"
echo "Working directory: $(pwd)"

# Print HTCondor/HEPSub environment variables
echo ""
echo "=== HEPSub Environment ==="
echo "Job ID: ${_CONDOR_IHEP_JOB_ID:-not set}"
echo "Remote host: ${_CONDOR_IHEP_REMOTE_HOST:-not set}"
echo "Submission time: ${_CONDOR_IHEP_SUBMISSION_TIME:-not set}"

echo ""
echo "=== Bootstrap script loaded ==="
echo ""

# Define the action function that Law expects
action() {
    # This function is called by Law's job script
    # Arguments: typically empty or job-specific parameters
    echo "Bootstrap action called with args: $@"

    # Set up Python environment (adjust for your needs)
    echo ""
    echo "=== Setting up Python environment ==="

    # Example: Load specific Python version
    # module load python/3.8  # Uncomment if using modules

    # Example: Activate virtual environment
    # if [ -f "$HOME/venv/bin/activate" ]; then
    #     source "$HOME/venv/bin/activate"
    #     echo "Activated virtual environment: $VIRTUAL_ENV"
    # fi

    # Example: Set PYTHONPATH
    # export PYTHONPATH="$HOME/myproject:$PYTHONPATH"

    # Set up software environment (adjust for your experiment)
    echo ""
    echo "=== Setting up software environment ==="

    # CMSSW environment setup (uncomment and configure for CMS)
    # Configure these environment variables in your workflow:
    #   LAW_CMSSW_VERSION: CMSSW version (e.g., "CMSSW_12_4_0")
    #   LAW_CMSSW_ARCH: SCRAM architecture (e.g., "slc7_amd64_gcc900")
    #   LAW_CMSSW_DIR: Base directory containing CMSSW installations
    if [ ! -z "${CMSSW_VERSION}" ]; then
        echo "CMSSW environment already available: ${CMSSW_VERSION}"
    else
        # Setup CMSSW if LAW_CMSSW_VERSION is defined
        if [ ! -z "${LAW_CMSSW_VERSION}" ]; then
            echo "Setting up CMSSW ${LAW_CMSSW_VERSION}"

            # Source CMSSW default setup
            if [ -f "/cvmfs/cms.cern.ch/cmsset_default.sh" ]; then
                source /cvmfs/cms.cern.ch/cmsset_default.sh
            fi

            # Set SCRAM_ARCH if specified
            [ ! -z "${LAW_CMSSW_ARCH}" ] && export SCRAM_ARCH="${LAW_CMSSW_ARCH}"

            # Go to CMSSW directory and setup environment
            if [ -d "${LAW_CMSSW_DIR}/${LAW_CMSSW_VERSION}" ]; then
                cd "${LAW_CMSSW_DIR}/${LAW_CMSSW_VERSION}/src"
                eval "$(scramv1 runtime -sh)"
                cd - > /dev/null
                echo "CMSSW ${LAW_CMSSW_VERSION} environment loaded"
            else
                echo "WARNING: CMSSW directory ${LAW_CMSSW_DIR}/${LAW_CMSSW_VERSION} not found"
            fi
        fi
    fi

    # Example for JUNO experiment
    # if [ "$HEPSUB_GROUP" = "juno" ]; then
    #     source /cvmfs/juno.ihep.ac.cn/centos7_amd64_gcc830/Pre-Release/J21v2r0-Pre2/setup.sh
    #     echo "JUNO environment loaded"
    # fi

    # Example for general physics group
    # export PATH="$HOME/bin:$PATH"
    # export LD_LIBRARY_PATH="$HOME/lib:$LD_LIBRARY_PATH"

    # Verify Python and required packages
    echo ""
    echo "=== Verifying environment ==="
    which python 2>/dev/null && python --version
    which pip 2>/dev/null && pip --version

    # Check if law is available
    if python -c "import law" 2>/dev/null; then
        echo "law is available"
        python -c "import law; print('law version:', law.__version__)"
    else
        echo "WARNING: law is not available in this environment"
    fi

    # Print system information
    echo ""
    echo "=== System Information ==="
    echo "CPU cores: $(nproc 2>/dev/null || echo 'unknown')"
    echo "Memory: $(free -h 2>/dev/null | grep Mem | awk '{print $2}' || echo 'unknown')"
    echo "Disk space: $(df -h . 2>/dev/null | tail -1 | awk '{print $4}' || echo 'unknown')"

    echo ""
    echo "=== Environment setup complete ==="
    echo ""

    return 0
}

# Call the action function (required by Law)
action "$@"
