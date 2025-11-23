# coding: utf-8

"""
HEPSub utilities.
"""

__all__ = ["get_hepsub_version"]


import re
import subprocess
import threading

from law.util import no_value, interruptable_popen


_hepsub_version = no_value
_hepsub_version_lock = threading.Lock()


def get_hepsub_version():
    """
    Returns the version of the HEPSub installation in a 3-tuple. The value is cached to accelerate
    repeated function invocations.
    """
    global _hepsub_version

    if _hepsub_version == no_value:
        version = None
        with _hepsub_version_lock:
            code, out, _ = interruptable_popen("hep_q -version", shell=True, executable="/bin/bash",
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            if code == 0:
                first_line = out.strip().split("\n")[0].strip()
                # Try to parse version, format may vary
                m = re.match(r"^.*?(\d+)\.(\d+)\.(\d+).*$", first_line)
                if m:
                    version = tuple(map(int, m.groups()))

            _hepsub_version = version

    return _hepsub_version
