# coding: utf-8

"""
HEPSub job manager for IHEP cluster.

This implementation is optimized for IHEP's workflow where jobs execute directly
in the submission directory with access to the shared filesystem. Jobs can directly
read input files and write output files without staging.

See https://afsapply.ihep.ac.cn/cchelp/en/local-cluster/jobs/HTCondor/.
"""

__all__ = ["HEPSubJobManager", "HEPSubJobFileFactory"]


import os
import stat
import time
import re
import subprocess

import six

from law.config import Config
from law.job.base import BaseJobManager, BaseJobFileFactory, JobInputFile, DeprecatedInputFiles
from law.target.file import get_path
from law.util import make_list, make_unique, quote_cmd, interruptable_popen
from law.logger import get_logger

from law.contrib.hepsub.util import get_hepsub_version


logger = get_logger(__name__)

_cfg = Config.instance()


class HEPSubJobManager(BaseJobManager):

    # chunking settings
    chunk_size_submit = 0
    chunk_size_cancel = _cfg.get_expanded_int("job", "hepsub_chunk_size_cancel")
    chunk_size_query = _cfg.get_expanded_int("job", "hepsub_chunk_size_query")

    submission_job_id_cre = re.compile(r".*submitted to cluster (\d+).*", re.IGNORECASE)

    def __init__(self, group=None, pool=None, universe=None, emails=False, threads=1):
        super(HEPSubJobManager, self).__init__()

        self.group = group
        self.pool = pool
        self.universe = universe
        self.emails = emails
        self.threads = threads

        # determine the HEPSub version once
        self.hepsub_version = get_hepsub_version()

    def cleanup(self, *args, **kwargs):
        raise NotImplementedError("HEPSubJobManager.cleanup is not implemented")

    def cleanup_batch(self, *args, **kwargs):
        raise NotImplementedError("HEPSubJobManager.cleanup_batch is not implemented")

    def submit(self, job_file, group=None, pool=None, universe=None, emails=None, retries=0,
            retry_delay=3, silent=False, _processes=None):
        # default arguments
        if group is None:
            group = self.group
        if pool is None:
            pool = self.pool
        if universe is None:
            universe = self.universe
        if emails is None:
            emails = self.emails

        # get the job file location as the submission command is run it the same directory
        job_file_dir, job_file_name = os.path.split(os.path.abspath(str(job_file)))

        # build the command
        cmd = ["hep_sub"]
        if group:
            cmd += ["-g", group]
        if pool:
            cmd += ["-p", pool]
        if universe:
            cmd += ["-u", universe]
        cmd.append(job_file_name)
        cmd = quote_cmd(cmd)

        # define the actual submission in a loop to simplify retries
        while True:
            # run the command
            logger.debug("submit hepsub job with command '{}'".format(cmd))
            code, out, err = interruptable_popen(cmd, shell=True, executable="/bin/bash",
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=job_file_dir, kill_timeout=2,
                processes=_processes)

            # get the job id
            if code == 0:
                m = self.submission_job_id_cre.search(out)
                if m:
                    job_id = m.group(1)
                else:
                    code = 1
                    err = "cannot parse job id from output:\n{}".format(out)

            # retry or done?
            if code == 0:
                return job_id

            logger.debug("submission of hepsub job '{}' failed with code {}:\n{}".format(
                job_file, code, err))

            if retries > 0:
                retries -= 1
                time.sleep(retry_delay)
                continue

            if silent:
                return None

            raise Exception("submission of hepsub job '{}' failed: \n{}".format(job_file, err))

    def cancel(self, job_id, silent=False, _processes=None):
        chunking = isinstance(job_id, (list, tuple))
        job_ids = make_list(job_id)

        # build the command
        cmd = ["hep_rm"]
        cmd += job_ids
        cmd = quote_cmd(cmd)

        # run it
        logger.debug("cancel hepsub job(s) with command '{}'".format(cmd))
        code, out, err = interruptable_popen(cmd, shell=True, executable="/bin/bash",
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, kill_timeout=2, processes=_processes)

        # check success
        if code != 0 and not silent:
            raise Exception("cancellation of hepsub job(s) '{}' failed with code {}:\n{}".format(
                job_id, code, err))

        return {job_id: None for job_id in job_ids} if chunking else None

    def query(self, job_id, silent=False, _processes=None):
        chunking = isinstance(job_id, (list, tuple))
        job_ids = make_list(job_id)

        # build the command
        cmd = ["hep_q", "-i"]
        cmd += job_ids
        cmd = quote_cmd(cmd)

        # run it
        logger.debug("query hepsub job(s) with command '{}'".format(cmd))
        code, out, err = interruptable_popen(cmd, shell=True, executable="/bin/bash",
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, kill_timeout=2, processes=_processes)

        # handle errors
        if code != 0:
            if silent:
                return None
            else:
                raise Exception("status query of hepsub job(s) '{}' failed with code {}:\n{}".format(
                    job_id, code, err))

        # parse the output and extract the status per job
        query_data = self.parse_query_output(out)

        # compare to the requested job ids and perform some checks
        for _job_id in job_ids:
            if _job_id not in query_data:
                if not chunking:
                    if silent:
                        return None
                    else:
                        raise Exception("hepsub job(s) '{}' not found in query response".format(
                            job_id))
                else:
                    query_data[_job_id] = self.job_status_dict(job_id=_job_id, status=self.FAILED,
                        error="job not found in query response")

        return query_data if chunking else query_data[job_id]

    @classmethod
    def parse_query_output(cls, out):
        """
        Example output to parse from hep_q:
        JOBID OWNER SUBMITTED RUN_TIME ST PRI SIZE CMD
        123456 user 1/1 12:00 0+00:01:23 R 0 100.0 job.sh
        """
        query_data = {}

        for line in out.strip().split("\n"):
            parts = line.split()
            if len(parts) < 5:
                continue

            # Skip header line
            if parts[0] == "JOBID":
                continue

            job_id = parts[0]
            status_flag = parts[4]

            # map the status
            status = cls.map_status(status_flag)

            # save the result
            query_data[job_id] = cls.job_status_dict(job_id=job_id, status=status)

        return query_data

    @classmethod
    def map_status(cls, status_flag):
        # HTCondor/HEPSub status mapping
        # I: Idle, R: Running, C: Completed, H: Held, X: Removed
        if status_flag in ("I", "H"):
            return cls.PENDING
        elif status_flag in ("R",):
            return cls.RUNNING
        elif status_flag in ("C",):
            return cls.FINISHED
        elif status_flag in ("X", "E"):
            return cls.FAILED
        else:
            return cls.FAILED


class HEPSubJobFileFactory(BaseJobFileFactory):

    config_attrs = BaseJobFileFactory.config_attrs + [
        "file_name", "command", "executable", "arguments", "group", "pool", "universe", "cwd",
        "input_files", "output_files", "postfix_output_files", "job_name", "stdout", "stderr",
        "shell", "emails", "custom_content", "memory", "cpus", "walltime",
    ]

    def __init__(self, file_name="hepsub_job.job", command=None, executable=None, arguments=None,
            group=None, pool=None, universe=None, cwd=None, input_files=None, output_files=None,
            postfix_output_files=True, job_name=None, stdout="stdout.txt", stderr="stderr.txt",
            shell="bash", emails=False, custom_content=None, memory=None, cpus=None,
            walltime=None, **kwargs):
        # get some default kwargs from the config
        cfg = Config.instance()
        if kwargs.get("dir") is None:
            kwargs["dir"] = cfg.get_expanded("job", cfg.find_option("job",
                "hepsub_job_file_dir", "job_file_dir"))
        if kwargs.get("mkdtemp") is None:
            kwargs["mkdtemp"] = cfg.get_expanded_bool("job", cfg.find_option("job",
                "hepsub_job_file_dir_mkdtemp", "job_file_dir_mkdtemp"))
        if kwargs.get("cleanup") is None:
            kwargs["cleanup"] = cfg.get_expanded_bool("job", cfg.find_option("job",
                "hepsub_job_file_dir_cleanup", "job_file_dir_cleanup"))

        super(HEPSubJobFileFactory, self).__init__(**kwargs)

        self.file_name = file_name
        self.command = command
        self.executable = executable
        self.arguments = arguments
        self.group = group
        self.pool = pool
        self.universe = universe
        self.cwd = cwd
        self.input_files = DeprecatedInputFiles(input_files or {})
        self.output_files = output_files or []
        self.postfix_output_files = postfix_output_files
        self.job_name = job_name
        self.stdout = stdout
        self.stderr = stderr
        self.shell = shell
        self.emails = emails
        self.custom_content = custom_content
        self.memory = memory
        self.cpus = cpus
        self.walltime = walltime

    def create(self, postfix=None, **kwargs):
        """
        Creates a HEPSub job file optimized for IHEP cluster.

        At IHEP, jobs run directly in the submission directory with access to the shared
        filesystem, so no file staging is needed. The job simply changes to the working
        directory and executes the command.
        """
        # merge kwargs and instance attributes
        c = self.get_config(**kwargs)

        # some sanity checks
        if not c.file_name:
            raise ValueError("file_name must not be empty")
        if not c.command and not c.executable:
            raise ValueError("either command or executable must not be empty")
        if not c.shell:
            raise ValueError("shell must not be empty")

        # ensure that the custom log file is an output file
        if c.custom_log_file and c.custom_log_file not in c.output_files:
            c.output_files.append(c.custom_log_file)

        # postfix certain output files
        c.output_files = list(map(str, c.output_files))
        if c.postfix_output_files:
            skip_postfix_cre = re.compile(r"^(/dev/).*$")
            skip_postfix = lambda s: bool(skip_postfix_cre.match(str(s)))
            c.output_files = [
                path if skip_postfix(path) else self.postfix_output_file(path, postfix)
                for path in c.output_files
            ]
            for attr in ["stdout", "stderr", "custom_log_file"]:
                if c[attr] and not skip_postfix(c[attr]):
                    c[attr] = self.postfix_output_file(c[attr], postfix)

        # ensure that all input files are JobInputFile objects
        c.input_files = {
            key: JobInputFile(f)
            for key, f in c.input_files.items()
        }

        # process input files - get absolute paths
        for key, f in c.input_files.items():
            abs_path = os.path.abspath(f.path)
            # copy to job directory if requested
            if f.copy:
                abs_path = self.provide_input(
                    src=abs_path,
                    postfix=postfix if f.postfix and not f.share else None,
                    dir=c.dir,
                    skip_existing=f.share,
                )
            f.path_sub_abs = abs_path
            f.path_job = abs_path

        # update render variables with input file paths
        c.render_variables.update({
            key: f.path_job
            for key, f in c.input_files.items()
        })

        # add the custom log file to render variables
        if c.custom_log_file:
            c.render_variables["log_file"] = c.custom_log_file

        # add the file postfix to render variables
        if postfix and "file_postfix" not in c.render_variables:
            c.render_variables["file_postfix"] = postfix

        # linearize render variables
        render_variables = self.linearize_render_variables(c.render_variables)

        # prepare the job description file
        job_file = self.postfix_input_file(os.path.join(c.dir, str(c.file_name)), postfix)

        # render input files if needed
        for key, f in c.input_files.items():
            if f.copy and f.render_local:
                self.render_file(
                    f.path_sub_abs,
                    f.path_sub_abs,
                    render_variables,
                    postfix=postfix if f.postfix else None,
                )

        # handle executable
        if c.executable:
            # find the executable in input files or use as-is
            executable_key = None
            for k, v in c.input_files.items():
                if get_path(v) == get_path(c.executable):
                    executable_key = k
                    break

            if executable_key:
                c.executable = c.input_files[executable_key].path_job
            else:
                c.executable = os.path.abspath(str(c.executable))

            # make the file executable
            if os.path.exists(c.executable):
                os.chmod(c.executable, os.stat(c.executable).st_mode | stat.S_IXUSR | stat.S_IXGRP)

        # build job file content
        content = []
        content.append("#!/usr/bin/env {}".format(c.shell))
        content.append("")
        content.append("# HEPSub job script for IHEP cluster")
        content.append("# Jobs run directly in the submission directory")
        content.append("")

        # add metadata as comments
        if c.job_name:
            content.append("# Job name: {}".format(c.job_name))
        if c.group:
            content.append("# Group: {}".format(c.group))
        if c.pool:
            content.append("# Pool: {}".format(c.pool))
        if c.universe:
            content.append("# Universe: {}".format(c.universe))
        if c.memory:
            content.append("# Memory: {} MB".format(c.memory))
        if c.cpus:
            content.append("# CPUs: {}".format(c.cpus))
        if c.walltime:
            content.append("# Walltime: {}".format(c.walltime))
        content.append("")

        # add custom content
        if c.custom_content:
            for item in c.custom_content:
                if isinstance(item, six.string_types):
                    content.append(item)
                else:
                    content.append(" ".join(str(x) for x in item))
            content.append("")

        # redirect stdout/stderr if needed
        if c.stdout:
            content.append("exec 1>{}".format(c.stdout))
        if c.stderr:
            content.append("exec 2>{}".format(c.stderr))
        if c.stdout or c.stderr:
            content.append("")

        # change to working directory
        # This is where the job actually runs and modifies files
        if c.cwd:
            content.append("# Change to working directory")
            content.append("cd {}".format(c.cwd))
            content.append("")

        # execute the command or executable
        if c.command:
            content.append(c.command)
        else:
            # use absolute path to executable
            content.append(c.executable)
        if c.arguments:
            args = quote_cmd(c.arguments) if isinstance(c.arguments, (list, tuple)) else c.arguments
            content[-1] += " {}".format(args)

        # write the job file
        with open(job_file, "w") as f:
            for line in content:
                f.write(line + "\n")

        # make the job file executable
        os.chmod(job_file, os.stat(job_file).st_mode | stat.S_IXUSR | stat.S_IXGRP)

        logger.debug("created hepsub job file at '{}'".format(job_file))

        return job_file, c
