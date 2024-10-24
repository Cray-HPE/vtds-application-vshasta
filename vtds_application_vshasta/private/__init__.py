#
# MIT License
#
# (C) Copyright [2024] Hewlett Packard Enterprise Development LP
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
"""Module initialization

"""

from os import sep as separator
from os.path import (
    join as path_join,
    dirname
)
CONFIG_DIR = path_join(dirname(__file__), 'config')
APP_CONFIG_NAME = 'application_core_config.yaml'
DEPLOY_SCRIPT_NAME = 'deploy_application_to_node.py'
SCRIPT_DIR_PATH = path_join(
    dirname(__file__),
    'scripts',
)


def script(filename):
    """Translate a file name into a full path name to a file in the
    scripts directory.

    """
    return path_join(SCRIPT_DIR_PATH, filename)


def home(filename):
    """Translate a filename into a full path on a remote host that is
    in the 'root' home directory.

    """
    return path_join(separator, "root", filename)
