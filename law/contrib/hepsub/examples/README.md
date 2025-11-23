# HEPSub Workflow Examples

This directory contains examples demonstrating how to use the `law.contrib.hepsub` module to submit jobs to the IHEP cluster using the HEPJob toolkit (`hep_sub`, `hep_q`, `hep_rm`).

## IHEP-Specific Design

**Important**: This implementation is optimized for IHEP's execution model where:
- Jobs run **directly in the submission directory**
- Worker nodes have access to the **shared filesystem** (NFS/AFS)
- **No file staging required** - jobs read and write files in place
- Simpler and more efficient than batch systems requiring file transfers

## Prerequisites

1. **Access to IHEP cluster**: You need access to the IHEP computing cluster
2. **HEPJob toolkit**: The `hep_sub`, `hep_q`, and `hep_rm` commands must be available
3. **Law installation**: Law must be installed with the hepsub contrib module

## Files

- **`example_workflow.py`**: Complete workflow examples showing various use cases
- **`bootstrap.sh`**: Example bootstrap script for setting up the job environment
- **`README.md`**: This file

## Quick Start

### 1. Basic Workflow Example

Run a simple workflow that processes 5 files:

```bash
# Submit jobs to the IHEP cluster
python example_workflow.py ProcessDataHEPSub \
    --n-files 5 \
    --hepsub-group juno \
    --workers 2

# Check job status
hep_q -u $USER

# After jobs complete, analyze results
python example_workflow.py AnalyzeResultsHEPSub --n-files 5
```

### 2. Custom Resource Requirements

Specify memory and walltime:

```bash
python example_workflow.py ProcessDataHEPSub \
    --n-files 10 \
    --hepsub-group juno \
    --hepsub-pool local \
    --memory 4096 \
    --walltime "04:00:00"
```

### 3. Simple Single Job

Submit a single job without using the full workflow:

```bash
python example_workflow.py SimpleHEPSubTask --hepsub-group physics
```

## Configuration

### Law Configuration File

Create a `law.cfg` file in your project directory:

```ini
[job]
# HEPSub job configuration
hepsub_job_file_dir: /path/to/job/files
hepsub_job_file_dir_mkdtemp: True
hepsub_job_file_dir_cleanup: False

# Chunk sizes for batch operations
hepsub_chunk_size_cancel: 25
hepsub_chunk_size_query: 25
```

### Task-Level Configuration

Configure your workflow task by overriding these methods:

```python
class MyTask(law.contrib.hepsub.HEPSubWorkflow):

    def hepsub_output_directory(self):
        """Where to store job files and outputs"""
        return law.LocalDirectoryTarget("output/jobs")

    def hepsub_bootstrap_file(self):
        """Bootstrap script to set up environment"""
        return law.JobInputFile("bootstrap.sh")

    def hepsub_job_config(self, config, job_num, branches):
        """Configure job parameters"""
        config.memory = 2048  # MB
        config.cpus = 1
        config.walltime = "02:00:00"
        return config
```

## HEPSub Parameters

### Task Parameters

- **`--hepsub-group`**: Job group (e.g., `physics`, `juno`, `dybrun`)
- **`--hepsub-pool`**: Resource pool (e.g., `virtual`, `local`, `ali`)
- **`--hepsub-universe`**: Job universe (e.g., `vanilla`, `grid`, `docker`)

### Available Groups at IHEP

Common groups you can use (check with your experiment):
- `physics` - General physics group
- `juno` - JUNO experiment
- `dybrun` - Daya Bay experiment
- `bes3` - BESIII experiment
- `cepc` - CEPC experiment

### Resource Pools

- `virtual` - Virtual machine pool
- `local` - Local cluster resources
- `ali` - Alibaba Cloud resources

## Job Monitoring

Monitor your jobs using HEPJob commands:

```bash
# Query all your jobs
hep_q

# Query specific job by ID
hep_q -i <job_id>

# Query only running jobs
hep_q -run

# Query by status (I=Idle, R=Running, C=Completed, H=Held, X=Removed)
hep_q -stat R

# Cancel a job
hep_rm <job_id>

# Cancel all your jobs
hep_rm -a

# Check walltime limits for your group
hep_clus -g juno --walltime
```

## Advanced Usage

### Custom Bootstrap Script

Edit `bootstrap.sh` to set up your specific environment:

```bash
#!/bin/bash

# Load your experiment's software
source /cvmfs/yourexperiment.ihep.ac.cn/setup.sh

# Activate Python virtual environment
source $HOME/venv/bin/activate

# Set environment variables
export MYAPP_DATA_DIR=/path/to/data

# Continue with job execution
action "$@"
```

### Using Input/Output Files (IHEP Model)

At IHEP, jobs run in the working directory with shared filesystem access:

```python
def hepsub_job_config(self, config, job_num, branches):
    # Input files are directly accessible - no staging needed
    # Just use absolute paths
    config.input_files["data"] = law.JobInputFile(
        "/afs/ihep.ac.cn/users/j/juno/data/input.root",
        copy=False,  # No need to copy, directly accessible
    )

    # Output files written directly to the working directory
    # The job will write to: <output_directory>/output.root
    config.output_files.append("output.root")

    # Working directory is set automatically to output_directory
    # config.cwd = "/path/to/output/directory"

    # Custom job script content
    config.custom_content = [
        "export MY_VAR=value",
        "echo 'Working in: '$(pwd)",
        "echo 'Files can be written directly here'",
    ]

    return config
```

**Key points**:
- Input files with `copy=False` are accessed via absolute paths
- Output files are written to `config.cwd` (the output directory)
- No staging overhead - files are read/written directly

### Debugging

Enable debug logging:

```bash
export LAW_LOG_LEVEL=DEBUG
python example_workflow.py ProcessDataHEPSub --n-files 1
```

Check job output files in the output directory:
- `stdout.txt` - Standard output
- `stderr.txt` - Standard error
- `stdall.txt` - Combined output (if transfer_logs=True)

## Troubleshooting

### Job Fails to Submit

1. Check that `hep_sub` command is available: `which hep_sub`
2. Verify your group membership: `hep_clus -g <group>`
3. Check job file is executable: `ls -l` in the job directory

### Job Gets Stuck in Idle (I) Status

1. Check resource requirements (memory, walltime)
2. Verify the requested pool has available resources
3. Check queue limits: `hep_clus -g <group>`

### Job Fails (X or E Status)

1. Check job output logs in the output directory
2. Verify bootstrap script doesn't have errors
3. Ensure required software is available on worker nodes

### Cannot Find Output Files

1. Check `hepsub_output_directory()` path is correct
2. Verify worker nodes can write to the output directory
3. Check for file transfer errors in job logs

## Examples Workflow Structure

```
ProcessDataHEPSub (Workflow)
├── Branch 0 → processes file_id=0
├── Branch 1 → processes file_id=1
├── Branch 2 → processes file_id=2
...
└── Branch N → processes file_id=N

AnalyzeResultsHEPSub (Task)
└── requires: ProcessDataHEPSub (all branches)
    └── analyzes all output files
```

## References

- [IHEP HTCondor Documentation](https://afsapply.ihep.ac.cn/cchelp/en/local-cluster/jobs/HTCondor/)
- [Law Documentation](https://law.readthedocs.io/)
- [Law Remote Workflows](https://law.readthedocs.io/en/latest/workflows.html#remote-workflows)

## Support

For issues specific to:
- **HEPSub/IHEP cluster**: Contact IHEP computing support
- **Law framework**: Check [Law GitHub issues](https://github.com/riga/law/issues)
- **This contrib module**: Check the law repository or open an issue
