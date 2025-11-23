# coding: utf-8

"""
HEPSub remote workflow implementation for IHEP cluster.

This implementation is optimized for IHEP's HEPSub system where jobs run directly
in the submission directory with access to the shared filesystem (NFS/AFS).
No file staging is required - jobs can directly read and write files in place.

See https://afsapply.ihep.ac.cn/cchelp/en/local-cluster/jobs/HTCondor/.
"""

__all__ = ["HEPSubWorkflow"]


import contextlib
from abc import abstractmethod
from collections import OrderedDict

import luigi
import six

from law.config import Config
from law.workflow.remote import BaseRemoteWorkflow, BaseRemoteWorkflowProxy
from law.job.base import JobArguments, JobInputFile, DeprecatedInputFiles
from law.task.proxy import ProxyCommand
from law.target.file import get_path, get_scheme, FileSystemDirectoryTarget
from law.target.local import LocalDirectoryTarget
from law.parameter import NO_STR
from law.util import no_value, law_src_path, merge_dicts, DotDict
from law.logger import get_logger

from law.contrib.hepsub.job import HEPSubJobManager, HEPSubJobFileFactory


logger = get_logger(__name__)


class HEPSubWorkflowProxy(BaseRemoteWorkflowProxy):

    workflow_type = "hepsub"

    def create_job_manager(self, **kwargs):
        return self.task.hepsub_create_job_manager(**kwargs)

    def create_job_file_factory(self, **kwargs):
        return self.task.hepsub_create_job_file_factory(**kwargs)

    def create_job_file(self, job_num, branches):
        task = self.task

        # the file postfix is pythonic range made from branches, e.g. [0, 1, 2, 4] -> "_0To5"
        postfix = "_{}To{}".format(branches[0], branches[-1] + 1)

        # create the config
        c = self.job_file_factory.get_config()
        c.input_files = DeprecatedInputFiles()
        c.output_files = []
        c.render_variables = {}
        c.custom_content = []

        # get the actual wrapper file that will be executed by the remote job
        wrapper_file = task.hepsub_wrapper_file()
        law_job_file = task.hepsub_job_file()
        if wrapper_file and get_path(wrapper_file) != get_path(law_job_file):
            c.input_files["executable_file"] = wrapper_file
            c.executable = wrapper_file
        else:
            c.executable = law_job_file
        c.input_files["job_file"] = law_job_file

        # collect task parameters
        exclude_args = (
            task.exclude_params_branch |
            task.exclude_params_workflow |
            task.exclude_params_remote_workflow |
            task.exclude_params_hepsub_workflow |
            {"workflow", "effective_workflow"}
        )
        proxy_cmd = ProxyCommand(
            task.as_branch(branches[0]),
            exclude_task_args=exclude_args,
            exclude_global_args=["workers", "local-scheduler", task.task_family + "-*"],
        )
        if task.hepsub_use_local_scheduler():
            proxy_cmd.add_arg("--local-scheduler", "True", overwrite=True)
        for key, value in OrderedDict(task.hepsub_cmdline_args()).items():
            proxy_cmd.add_arg(key, value, overwrite=True)

        # job script arguments
        job_args = JobArguments(
            task_cls=task.__class__,
            task_params=proxy_cmd.build(skip_run=True),
            branches=branches,
            workers=task.job_workers,
            auto_retry=False,
            dashboard_data=self.dashboard.remote_hook_data(
                job_num, self.job_data.attempts.get(job_num, 0)),
        )
        c.arguments = job_args.join()

        # add the bootstrap file
        bootstrap_file = task.hepsub_bootstrap_file()
        if bootstrap_file:
            c.input_files["bootstrap_file"] = bootstrap_file

        # add the stageout file
        stageout_file = task.hepsub_stageout_file()
        if stageout_file:
            c.input_files["stageout_file"] = stageout_file

        # does the dashboard have a hook file?
        dashboard_file = self.dashboard.remote_hook_file()
        if dashboard_file:
            c.input_files["dashboard_file"] = dashboard_file

        # initialize logs with empty values and defer to defaults later
        c.stdout = no_value
        c.stderr = no_value
        if task.transfer_logs:
            c.custom_log_file = "stdall.txt"

        # helper to cast directory paths to local directory targets if possible
        def cast_dir(output_dir, touch=True):
            if not isinstance(output_dir, FileSystemDirectoryTarget):
                path = get_path(output_dir)
                if get_scheme(path) not in (None, "file"):
                    return output_dir
                output_dir = LocalDirectoryTarget(path)
            if touch:
                output_dir.touch()
            return output_dir

        # At IHEP, jobs run directly in the submission/working directory
        # Set cwd to the output directory where job will execute and write files
        output_dir = cast_dir(task.hepsub_output_directory())
        output_dir_is_local = isinstance(output_dir, LocalDirectoryTarget)
        if output_dir_is_local:
            c.cwd = output_dir.abspath

        # job name
        c.job_name = "{}{}".format(task.live_task_id, postfix)

        # task hook
        c = task.hepsub_job_config(c, job_num, branches)

        # when the output dir is not local, direct output files are not possible
        if not output_dir_is_local:
            del c.output_files[:]

        # build the job file and get the sanitized config
        job_file, c = self.job_file_factory(postfix=postfix, **c.__dict__)

        # logging defaults
        # we do not use hepsub's logging mechanism since it might require that the submission
        # directory is present when it retrieves logs, and therefore we use a custom log file
        c.stdout = c.stdout or None
        c.stderr = c.stderr or None
        c.custom_log_file = c.custom_log_file or None

        # get the location of the custom local log file if any
        abs_log_file = None
        if output_dir_is_local and c.custom_log_file:
            abs_log_file = output_dir.child(c.custom_log_file, type="f").abspath

        # return job and log files
        return {"job": job_file, "config": c, "log": abs_log_file}

    def destination_info(self):
        info = super(HEPSubWorkflowProxy, self).destination_info()

        if self.task.hepsub_group != NO_STR:
            info["group"] = "group: {}".format(self.task.hepsub_group)

        if self.task.hepsub_pool != NO_STR:
            info["pool"] = "pool: {}".format(self.task.hepsub_pool)

        info = self.task.hepsub_destination_info(info)

        return info


class HEPSubWorkflow(BaseRemoteWorkflow):

    workflow_proxy_cls = HEPSubWorkflowProxy

    hepsub_workflow_run_decorators = None
    hepsub_job_manager_defaults = None
    hepsub_job_file_factory_defaults = None

    hepsub_group = luigi.Parameter(
        default=NO_STR,
        significant=False,
        description="target hepsub group; default: empty",
    )

    hepsub_pool = luigi.Parameter(
        default=NO_STR,
        significant=False,
        description="target hepsub pool; default: empty",
    )

    hepsub_universe = luigi.Parameter(
        default=NO_STR,
        significant=False,
        description="target hepsub universe; default: empty",
    )

    hepsub_job_kwargs = ["hepsub_group", "hepsub_pool", "hepsub_universe"]
    hepsub_job_kwargs_submit = None
    hepsub_job_kwargs_cancel = None
    hepsub_job_kwargs_query = None

    exclude_params_branch = {"hepsub_group", "hepsub_pool", "hepsub_universe"}

    exclude_params_hepsub_workflow = set()

    exclude_index = True

    @contextlib.contextmanager
    def hepsub_workflow_run_context(self):
        """
        Hook to provide a context manager in which the workflow run implementation is placed. This
        can be helpful in situations where resurces should be acquired before and released after
        running a workflow.
        """
        yield

    @abstractmethod
    def hepsub_output_directory(self):
        """
        Hook to define the location of submission output files, such as the json files containing
        job data, and optional log files.
        This method should return a :py:class:`FileSystemDirectoryTarget`.
        """
        return None

    def hepsub_bootstrap_file(self):
        return None

    def hepsub_wrapper_file(self):
        return None

    def hepsub_job_file(self):
        return JobInputFile(law_src_path("job", "law_job.sh"))

    def hepsub_stageout_file(self):
        return None

    def hepsub_workflow_requires(self):
        return DotDict()

    def hepsub_output_postfix(self):
        return ""

    def hepsub_job_resources(self, job_num, branches):
        """
        Hook to define resources for a specific job with number *job_num*, processing *branches*.
        This method should return a dictionary.
        """
        return {}

    def hepsub_job_manager_cls(self):
        return HEPSubJobManager

    def hepsub_create_job_manager(self, **kwargs):
        kwargs = merge_dicts(self.hepsub_job_manager_defaults, kwargs)
        return self.hepsub_job_manager_cls()(**kwargs)

    def hepsub_job_file_factory_cls(self):
        return HEPSubJobFileFactory

    def hepsub_create_job_file_factory(self, **kwargs):
        # get the file factory cls
        factory_cls = self.hepsub_job_file_factory_cls()

        # job file fectory config priority: kwargs > class defaults
        kwargs = merge_dicts({}, self.hepsub_job_file_factory_defaults, kwargs)

        # default mkdtemp value which might require task-level info
        if kwargs.get("mkdtemp") is None:
            cfg = Config.instance()
            mkdtemp = cfg.get_expanded(
                "job",
                cfg.find_option("job", "hepsub_job_file_dir_mkdtemp", "job_file_dir_mkdtemp"),
            )
            if isinstance(mkdtemp, six.string_types) and mkdtemp.lower() not in {"true", "false"}:
                kwargs["mkdtemp"] = factory_cls._expand_template_path(
                    mkdtemp,
                    variables={"task_id": self.live_task_id, "task_family": self.task_family},
                )

        return factory_cls(**kwargs)

    def hepsub_job_config(self, config, job_num, branches):
        return config

    def hepsub_dump_intermediate_job_data(self):
        """
        Whether to dump intermediate job data to the job submission file while jobs are being
        submitted.
        """
        return True

    def hepsub_post_submit_delay(self):
        """
        Configurable delay in seconds to wait after submitting jobs and before starting the status
        polling.
        """
        return self.poll_interval * 60

    def hepsub_check_job_completeness(self):
        """
        Check if jobs completed by looking at output files.
        Essential for IHEP where fast jobs finish before we can query them.
        """
        return True

    def hepsub_check_job_completeness_delay(self):
        """
        Delay before checking output files (in seconds).
        Give the job a moment to write outputs after completion.
        """
        return 10.0

    def hepsub_poll_callback(self, poll_data):
        """
        Configurable callback that is called after each job status query and before potential
        resubmission. It receives the variable polling attributes *poll_data* (:py:class:`PollData`)
        that can be changed within this method.

        If *False* is returned, the polling loop is gracefully terminated. Returning any other value
        does not have any effect.
        """
        return

    def hepsub_use_local_scheduler(self):
        # try to use the config setting
        return Config.instance().get_expanded_bool("luigi_core", "local_scheduler", False)

    def hepsub_cmdline_args(self):
        return {}

    def hepsub_destination_info(self, info):
        return info
