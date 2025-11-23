# HEPSub Implementation Design

## IHEP-Optimized Execution Model

This implementation is specifically optimized for the IHEP cluster's execution model, which differs significantly from traditional batch systems like LSF or SLURM.

### Key Characteristics

#### 1. **Direct Execution in Submission Directory**

At IHEP, when you submit a job from `/path/to/work`, the job executes in that same directory:

```bash
# Traditional batch system (e.g., LSF)
$ pwd
/home/user/submit/dir
$ bsub job.sh
# Job runs in: /scratch/node123/temp_dir  (different location!)

# IHEP HEPSub system
$ pwd
/afs/ihep.ac.cn/users/u/username/work
$ hep_sub job.sh
# Job runs in: /afs/ihep.ac.cn/users/u/username/work  (same location!)
```

#### 2. **Shared Filesystem Access**

Worker nodes have direct access to the shared filesystem (NFS/AFS):
- No need to stage input files to worker nodes
- No need to stage output files back
- Files are directly accessible via absolute paths
- Changes are immediately visible

#### 3. **Simplified Job Scripts**

Traditional batch system job script:
```bash
#!/bin/bash
# Stage in input files
cp $LSB_SUB_CWD/input.dat .

# Run computation
./process input.dat > output.dat

# Stage out output files
cp output.dat $LSB_SUB_CWD/
```

IHEP HEPSub job script:
```bash
#!/bin/bash
# Change to working directory
cd /afs/ihep.ac.cn/users/u/username/work

# Run computation - files already here!
./process input.dat > output.dat

# Done - output.dat already in final location!
```

## Implementation Differences from LSF

### Removed Features
- `manual_stagein` / `manual_stageout` - Not needed
- `absolute_paths` flag - Always use absolute paths
- File transfer commands in job scripts
- `$LS_EXECCWD` type environment variable handling

### Simplified Features

#### Job File Factory
```python
# LSF approach (complex)
- Manage input/output file staging
- Handle relative vs absolute paths
- Copy files to/from execution directory
- Generate stage-in/stage-out commands

# HEPSub approach (simple)
- Use absolute paths directly
- Set cwd to working directory
- Execute command in place
- Done!
```

#### Configuration
```python
# What you need to specify
config.cwd = "/path/to/working/directory"  # Where job runs
config.input_files["data"] = JobInputFile(
    "/afs/ihep.ac.cn/data/input.root",
    copy=False,  # Direct access, no copy needed
)
config.output_files.append("output.root")  # Written to cwd
```

## Advantages

### 1. **Performance**
- No file transfer overhead
- Instant access to input data
- Output immediately available
- Reduced I/O on worker nodes

### 2. **Simplicity**
- Fewer configuration options
- Easier to understand job flow
- Less that can go wrong
- Easier debugging (files in predictable locations)

### 3. **Disk Space**
- No duplication of input files
- No temporary copies on worker nodes
- Efficient use of shared storage

### 4. **Reliability**
- No transfer failures
- No "lost" files on worker nodes
- Consistent file locations
- Easier error recovery

## Usage Patterns

### Pattern 1: Process Data in Place

```python
class ProcessData(HEPSubWorkflow):
    def hepsub_output_directory(self):
        # Jobs run here and write outputs here
        return law.LocalDirectoryTarget("/afs/ihep.ac.cn/users/u/user/analysis")

    def run(self):
        # Running in /afs/ihep.ac.cn/users/u/user/analysis
        # Read input directly
        data = read_input("/afs/ihep.ac.cn/data/input.root")

        # Process
        result = process(data)

        # Write output directly (already in output_directory)
        write_output("output.root", result)
```

### Pattern 2: Multiple Jobs, Shared Data

```python
class ParallelAnalysis(HEPSubWorkflow):
    def create_branch_map(self):
        return {i: {"run": i} for i in range(100)}

    def hepsub_output_directory(self):
        return law.LocalDirectoryTarget("/afs/ihep.ac.cn/users/u/user/results")

    def run(self):
        run_id = self.branch_data["run"]

        # All jobs read from same shared location - no duplication!
        common_data = read("/afs/ihep.ac.cn/data/common.root")
        run_data = read("/afs/ihep.ac.cn/data/run_{}.root".format(run_id))

        # Process
        result = analyze(common_data, run_data)

        # Each job writes its own output
        write("result_{}.root".format(run_id), result)
```

## Limitations and Considerations

### 1. **Shared Filesystem Required**
- Only works with shared storage (NFS/AFS)
- Won't work with local-only storage
- Not suitable for systems without shared filesystem

### 2. **Concurrent Access**
- Multiple jobs may access same directory
- Need proper file naming to avoid conflicts
- Consider using branch IDs in output filenames

### 3. **Network I/O**
- All I/O goes over network filesystem
- May be slower than local disk for small files
- Good for: large files, sequential access
- Watch out for: many small files, random access

### 4. **Disk Quotas**
- All output accumulates in shared storage
- Monitor disk usage
- Clean up intermediate files

## Best Practices

### 1. **Use Unique Output Names**
```python
def output(self):
    # Include branch ID to avoid conflicts
    return law.LocalFileTarget(
        os.path.join(
            self.output_directory.path,
            "output_branch_{}.root".format(self.branch)
        )
    )
```

### 2. **Set Working Directory Explicitly**
```python
def hepsub_output_directory(self):
    # Be explicit about where jobs run
    return law.LocalDirectoryTarget(
        "/afs/ihep.ac.cn/users/u/username/project/output"
    )
```

### 3. **Use Absolute Paths for Shared Data**
```python
def hepsub_job_config(self, config, job_num, branches):
    # Use absolute paths for shared input data
    config.custom_content = [
        "export DATA_DIR=/afs/ihep.ac.cn/data/experiment",
        "export CALIB_FILE=/afs/ihep.ac.cn/calib/latest.root",
    ]
    return config
```

### 4. **Monitor Filesystem Performance**
- Check network filesystem load
- Use `df -h` to monitor disk usage
- Consider I/O patterns in job design

## Comparison Summary

| Feature | Traditional (LSF) | IHEP (HEPSub) |
|---------|------------------|---------------|
| File staging | Required | Not needed |
| Job execution location | Temp directory on worker | Submission directory |
| Input file access | Copy to worker node | Direct access via NFS/AFS |
| Output file handling | Copy back from worker | Write directly to final location |
| Configuration complexity | High (many options) | Low (simple and direct) |
| Disk space usage | Higher (copies) | Lower (no copies) |
| Performance overhead | File transfer time | Network filesystem I/O |
| Best for | Isolated execution | Shared data processing |

## Migration from LSF

If you have LSF workflows, here's how to adapt them:

```python
# LSF workflow
class MyLSFTask(LSFWorkflow):
    def lsf_job_config(self, config, job_num, branches):
        config.manual_stagein = True
        config.manual_stageout = True
        config.input_files["data"] = JobInputFile("/data/input.root", copy=True)
        return config

# HEPSub equivalent (much simpler!)
class MyHEPSubTask(HEPSubWorkflow):
    def hepsub_job_config(self, config, job_num, branches):
        # No staging needed - just use the file directly!
        config.input_files["data"] = JobInputFile(
            "/afs/ihep.ac.cn/data/input.root",
            copy=False
        )
        return config
```

## Future Enhancements

Potential improvements for specific use cases:

1. **Local scratch space support** - For temp files during processing
2. **Batch file operations** - Optimized for many small files
3. **Checkpoint/restart** - For long-running jobs
4. **Resource monitoring** - Track NFS/AFS I/O usage

---

**Summary**: The HEPSub implementation is streamlined for IHEP's shared-filesystem model, removing unnecessary complexity and providing better performance for the typical use case of processing shared data with multiple jobs.
