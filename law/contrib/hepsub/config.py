# coding: utf-8

"""
Function returning the config defaults of the hepsub package.
"""


def config_defaults(default_config):
    return {
        "job": {
            "hepsub_job_file_dir": None,
            "hepsub_job_file_dir_mkdtemp": None,
            "hepsub_job_file_dir_cleanup": False,
            "hepsub_chunk_size_cancel": 25,
            "hepsub_chunk_size_query": 25,
        },
    }
