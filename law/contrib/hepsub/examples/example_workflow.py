#!/usr/bin/env python
# coding: utf-8

"""
Example HEPSub workflow demonstrating job submission to IHEP cluster.

This example shows:
1. Basic HEPSubWorkflow setup
2. How to configure job parameters (group, pool, resources)
3. File staging and output collection
4. Using bootstrap scripts
"""

import os
import law
import luigi

# Make sure law.contrib.hepsub is loaded
law.contrib.load("hepsub")


class ProcessDataHEPSub(law.contrib.hepsub.HEPSubWorkflow, law.LocalWorkflow):
    """
    Example workflow that processes data files on the IHEP cluster using hep_sub.

    Each branch processes one data chunk and produces an output file.
    """

    # Task parameters
    hepsub_group = luigi.Parameter(
        default="cms",
        description="HEPSub job group (e.g., physics, cms, juno, dybrun)",
    )

    hepsub_pool = luigi.Parameter(
        default="",
        description="HEPSub resource pool (e.g., virtual, local, ali)",
    )

    n_files = luigi.IntParameter(
        default=10,
        description="number of files to process",
    )

    memory = luigi.IntParameter(
        default=2048,
        description="memory requirement in MB",
    )

    hepsub_walltime = luigi.Parameter(
        default="02:00:00",
        description="walltime for hep_sub jobs (HH:MM:SS format)",
    )

    # Workflow methods
    def create_branch_map(self):
        """
        Define branches - each branch processes one file.
        """
        return {i: {"file_id": i} for i in range(self.n_files)}

    def output(self):
        """
        Define output targets for each branch.
        """
        return law.LocalFileTarget(
            os.path.join(
                self.hepsub_output_directory().path,
                "output_branch_{}.txt".format(self.branch),
            ),
        )

    def run(self):
        """
        The actual task logic that runs on the remote worker node.
        """
        branch_data = self.branch_map[self.branch]
        file_id = branch_data["file_id"]

        # Simulate some processing
        output_text = []
        output_text.append("Processing file_id: {}".format(file_id))
        output_text.append("Running on host: {}".format(os.getenv("HOSTNAME", "unknown")))
        output_text.append("Job ID: {}".format(os.getenv("_CONDOR_IHEP_JOB_ID", "unknown")))
        output_text.append("Submission time: {}".format(
            os.getenv("_CONDOR_IHEP_SUBMISSION_TIME", "unknown")))

        # Do some computation
        result = sum(range(file_id * 1000, (file_id + 1) * 1000))
        output_text.append("Computation result: {}".format(result))

        # Write output
        output = self.output()
        output.parent.touch()
        with output.open("w") as f:
            f.write("\n".join(output_text) + "\n")

    # HEPSub-specific configuration methods
    def hepsub_output_directory(self):
        """
        Directory where job files and outputs are stored.
        """
        return law.LocalDirectoryTarget(
            os.path.join(
                os.path.expandvars("$HOME"),
                "law_hepsub_jobs",
                self.task_family,
            ),
        )

    def hepsub_bootstrap_file(self):
        """
        Bootstrap script that sets up the environment on the worker node.
        """
        bootstrap_file = os.path.join(
            os.path.dirname(__file__),
            "bootstrap.sh",
        )
        if os.path.exists(bootstrap_file):
            return law.JobInputFile(bootstrap_file, share=True, render_job=False)
        return None

    def hepsub_job_config(self, config, job_num, branches):
        """
        Configure individual job settings.
        """
        # Set resource requirements
        config.memory = self.memory
        config.cpus = 1
        config.walltime = self.hepsub_walltime

        # Add custom content to the job script
        config.custom_content = [
            "# Job for branches: {}".format(branches),
            "echo 'Starting job on node: '$(hostname)",
            "echo 'Working directory: '$(pwd)",
            "",
        ]

        return config

    def hepsub_use_local_scheduler(self):
        """
        Use local scheduler for this example (no central Luigi scheduler needed).
        """
        return True


class AnalyzeResultsHEPSub(law.Task):
    """
    Downstream task that analyzes all results from the HEPSub workflow.
    """

    hepsub_group = luigi.Parameter(default="cms")
    n_files = luigi.IntParameter(default=10)

    def requires(self):
        """
        Require all branches of the ProcessDataHEPSub workflow to complete.
        """
        return ProcessDataHEPSub(
            hepsub_group=self.hepsub_group,
            n_files=self.n_files,
        )

    def output(self):
        return law.LocalFileTarget("analysis_summary.txt")

    def run(self):
        """
        Collect and analyze all results.
        """
        inputs = self.input()["collection"].targets

        summary = []
        summary.append("Analysis Summary")
        summary.append("=" * 50)
        summary.append("Total files processed: {}".format(len(inputs)))
        summary.append("")

        total_result = 0
        for branch_id, target in inputs.items():
            with target.open("r") as f:
                content = f.read()
                # Extract the result line
                for line in content.split("\n"):
                    if "Computation result:" in line:
                        result = int(line.split(":")[-1].strip())
                        total_result += result
                        summary.append("Branch {}: result = {}".format(branch_id, result))

        summary.append("")
        summary.append("Total sum: {}".format(total_result))

        # Write summary
        output = self.output()
        output.parent.touch()
        with output.open("w") as f:
            f.write("\n".join(summary) + "\n")


# Example of a simple standalone task (non-workflow)
class SimpleHEPSubTask(law.Task):
    """
    Simple single-job example using HEPSubJobManager directly.
    """

    hepsub_group = luigi.Parameter(default="cms")

    def output(self):
        return law.LocalFileTarget("simple_output.txt")

    def run(self):
        """
        This shows how to use HEPSubJobManager directly for single jobs.
        """
        from law.contrib.hepsub import HEPSubJobManager, HEPSubJobFileFactory

        # Create a job manager
        job_manager = HEPSubJobManager(
            group=self.hepsub_group,
            threads=1,
        )

        # Create job file factory
        job_factory = HEPSubJobFileFactory(
            dir=os.path.expandvars("$HOME/law_hepsub_jobs/simple"),
            mkdtemp=False,
        )

        # Create a simple job script
        job_file, config = job_factory(
            executable="echo",
            arguments=["'Hello from IHEP cluster!'", ">", "simple_output.txt"],
            stdout="job_stdout.txt",
            stderr="job_stderr.txt",
        )

        # Submit the job
        job_id = job_manager.submit(job_file, group=self.hepsub_group)
        print("Submitted job with ID: {}".format(job_id))

        # Note: In a real scenario, you would poll for completion
        # For this example, we just mark it as done
        self.output().touch()


if __name__ == "__main__":
    """
    Run the example workflow.

    Usage:
        # Run the full workflow with 5 files
        python example_workflow.py ProcessDataHEPSub --n-files 5 --hepsub-group juno

        # Run analysis after workflow completes
        python example_workflow.py AnalyzeResultsHEPSub --n-files 5

        # Run a simple single job
        python example_workflow.py SimpleHEPSubTask
    """
    law.cli.run()
