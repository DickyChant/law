# HEPSub Quick Start Guide

Get started with HEPSub workflows in 5 minutes!

## IHEP Execution Model

At IHEP, jobs run **directly in your working directory** with access to the shared filesystem (NFS/AFS). This means:
- ✅ No file staging needed
- ✅ Direct read/write to shared storage
- ✅ Simpler job scripts
- ✅ Files modified in place

## Installation

Ensure law is installed with the hepsub contrib:

```bash
# Law should already be installed in your environment
python -c "import law; law.contrib.load('hepsub'); print('OK')"
```

## Run Your First Workflow

### Step 1: Navigate to the examples directory

```bash
cd law/contrib/hepsub/examples/
```

### Step 2: Run the example workflow

```bash
# Submit 3 jobs to the IHEP cluster
./example_workflow.py ProcessDataHEPSub \
    --n-files 3 \
    --hepsub-group juno \
    --workers 2
```

### Step 3: Monitor your jobs

```bash
# Check job status
hep_q

# Watch jobs in real-time
watch -n 5 hep_q
```

### Step 4: Analyze results

```bash
# After jobs complete, run analysis
./example_workflow.py AnalyzeResultsHEPSub --n-files 3

# View the summary
cat analysis_summary.txt
```

## Common Commands

```bash
# Submit workflow with custom settings
./example_workflow.py ProcessDataHEPSub \
    --n-files 10 \
    --hepsub-group physics \
    --hepsub-pool local \
    --memory 4096 \
    --walltime "04:00:00"

# Check workflow status (law built-in)
law run ProcessDataHEPSub --n-files 10 --print-status -1

# Submit with more workers for parallel submission
./example_workflow.py ProcessDataHEPSub \
    --n-files 20 \
    --workers 4

# Run simple single job test
./example_workflow.py SimpleHEPSubTask --hepsub-group juno
```

## Job Management

```bash
# View all your jobs
hep_q

# View specific job
hep_q -i <job_id>

# Cancel a job
hep_rm <job_id>

# Cancel all your jobs
hep_rm -a

# Check group walltime limits
hep_clus -g juno --walltime
```

## Customization

### Change the HEPSub group

Edit `law.cfg`:
```ini
[ProcessDataHEPSub]
hepsub_group: your_group_name
```

Or use command line:
```bash
./example_workflow.py ProcessDataHEPSub --hepsub-group your_group_name
```

### Customize bootstrap script

Edit `bootstrap.sh` to set up your environment:
- Load required modules
- Activate virtual environments
- Set environment variables
- Add experiment-specific setup

### Adjust resource requirements

Modify in `example_workflow.py`:
```python
def hepsub_job_config(self, config, job_num, branches):
    config.memory = 4096  # 4 GB
    config.cpus = 2       # 2 CPU cores
    config.walltime = "08:00:00"  # 8 hours
    return config
```

## Output Files

Job outputs are stored in:
```
$HOME/law_hepsub_jobs/ProcessDataHEPSub/
├── submission_<timestamp>.json    # Job tracking data
├── hepsub_job_*.job              # Job scripts
├── stdout_*.txt                  # Job stdout
├── stderr_*.txt                  # Job stderr
└── output_branch_*.txt           # Task outputs
```

## Troubleshooting

**Problem**: Jobs stay in Idle (I) status
- Check resource availability: `hep_clus -g <your_group>`
- Reduce memory/walltime requirements
- Try a different pool

**Problem**: Cannot import law.contrib.hepsub
- Ensure hepsub module exists: `ls law/contrib/hepsub/`
- Load it explicitly: `law.contrib.load("hepsub")`

**Problem**: Job fails immediately
- Check bootstrap.sh for errors
- Verify job script permissions: `ls -l <job_file>`
- Review stderr logs

## Next Steps

1. Read the full [README.md](README.md) for detailed documentation
2. Customize `example_workflow.py` for your use case
3. Check [IHEP HTCondor docs](https://afsapply.ihep.ac.cn/cchelp/en/local-cluster/jobs/HTCondor/)
4. Explore [Law documentation](https://law.readthedocs.io/)

## Example Output

When successful, you'll see:
```
INFO: Running ProcessDataHEPSub(n_files=3)
INFO: Submitted job 123456 for branches [0]
INFO: Submitted job 123457 for branches [1]
INFO: Submitted job 123458 for branches [2]
INFO: Polling job status...
INFO: 3/3 jobs completed
INFO: Task ProcessDataHEPSub(n_files=3) complete!
```

Happy computing on the IHEP cluster! 🚀
