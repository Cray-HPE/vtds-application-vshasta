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
"""Deployment script for setting up Virtual Shasta on nodes

"""
from deploy_application_common import (
    UsageError,
    read_config,
    add_hosts,
    install_deb_packages,
    entrypoint,
)


PYTHON = "python3"


def main(argv):
    """Main entry point.

    """
    # Arguments are 'blade_class' the name of the blade class to which
    # this blade belongs and 'config_path' the path to the
    # configuration file used for this deployment.
    if not argv:
        raise UsageError("no arguments provided")
    if len(argv) < 2:
        raise UsageError("too few arguments")
    if len(argv) > 2:
        raise UsageError("too many arguments")

    config = read_config(argv[1])
    add_hosts(config)
    install_deb_packages(config)


if __name__ == '__main__':
    USAGE_MSG = """
usage: deploy_application_to_node node_class config_path

Where:

    node_class  is the name of the Virtual Node class to which this
                Virtual Node belongs.
    config_path is the path to a YAML file containing the application
                configuration to apply.
"""[1:-1]
    entrypoint(USAGE_MSG, main)
