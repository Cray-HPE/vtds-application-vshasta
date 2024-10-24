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
import sys
import os
from os.path import (
    join as path_join
)
from subprocess import (
    Popen,
    TimeoutExpired
)
import yaml


VENV_PATH = path_join(os.sep, "root", "venv")
REQUIREMENTS_PATH = path_join(os.sep, "root", "requirements.txt")
PYTHON = path_join(VENV_PATH, "bin", "python3")


class ContextualError(Exception):
    """Exception to report failures seen and contextualized within the
    application.

    """


class UsageError(Exception):  # pylint: disable=too-few-public-methods
    """Exception to report usage errors

    """


def write_out(string):
    """Write an arbitrary string on stdout and make sure it is
    flushed.

    """
    sys.stdout.write(string)
    sys.stdout.flush()


def write_err(string):
    """Write an arbitrary string on stderr and make sure it is
    flushed.

    """
    sys.stderr.write(string)
    sys.stderr.flush()


def usage(usage_msg, err=None):
    """Print a usage message and exit with an error status.

    """
    if err:
        write_err("ERROR: %s\n" % err)
    write_err("%s\n" % usage_msg)
    sys.exit(1)


def error_msg(msg):
    """Format an error message and print it to stderr.

    """
    write_err("ERROR: %s\n" % msg)


def warning_msg(msg):
    """Format a warning and print it to stderr.

    """
    write_err("WARNING: %s\n" % msg)


def info_msg(msg):
    """Format an informational message and print it to stderr.

    """
    write_err("INFO: %s\n" % msg)


def run_cmd(cmd, args, stdin=sys.stdin, check=True, timeout=None):
    """Run a command with output on stdout and errors on stderr

    """
    exitval = 0
    try:
        with Popen(
                [cmd, *args],
                stdin=stdin, stdout=sys.stdout, stderr=sys.stderr
        ) as command:
            time = 0
            signaled = False
            while True:
                try:
                    exitval = command.wait(timeout=5)
                except TimeoutExpired:
                    time += 5
                    if timeout and time > timeout:
                        if not signaled:
                            # First try to terminate the process
                            command.terminate()
                            continue
                        command.kill()
                        print()
                        # pylint: disable=raise-missing-from
                        raise ContextualError(
                            "'%s' timed out and did not terminate "
                            "as expected after %d seconds" % (
                                " ".join([cmd, *args]),
                                time
                            )
                        )
                    continue
                # Didn't time out, so the wait is done.
                break
            print()
    except OSError as err:
        raise ContextualError(
            "executing '%s' failed - %s" % (
                " ".join([cmd, *args]),
                str(err)
            )
        ) from err
    if exitval != 0 and check:
        fmt = (
            "command '%s' failed"
            if not signaled
            else "command '%s' timed out and was killed"
        )
        raise ContextualError(fmt % " ".join([cmd, *args]))
    return exitval


def read_config(config_file):
    """Read in the specified YAML configuration file for this blade
    and return the parsed data.

    """
    try:
        with open(config_file, 'r', encoding='UTF-8') as config:
            return yaml.safe_load(config)
    except OSError as err:
        raise ContextualError(
            "failed to load blade configuration file '%s' - %s" % (
                config_file,
                str(err)
            )
        ) from err


def add_hosts(config):
    """Add the host entries provided by the configuration to
    /etc/hosts

    """
    host_map = config.get('host_ipv4_map', {})
    with open("/etc/hosts", 'a', encoding='UTF-8') as hosts:
        hosts.write("# Added by vTDS Application Layer Deployment\n")
        for alias, ipaddr in host_map.items():
            hosts.write("%-15.15s %s\n" % (ipaddr, alias))


def install_deb_packages(config):
    """Initialize 'apt' and install the required debian packages as
    listed in the configuration.

    """
    packages = config.get('debian_packages', [])
    run_cmd('apt', ['update'])
    run_cmd('apt', ['install', '-y', *packages])


def create_venv():
    """Make a virtual environment for python to run in...

    """
    # do the import here because we can't do it at the top of the file
    # since the package hasn't been installed until we get the debian
    # packages installed.
    #
    # pylint: disable=import-outside-toplevel
    from venv import EnvBuilder
    EnvBuilder(
        system_site_packages=False,
        clear=True,
        symlinks=False,
        upgrade=False,
        with_pip=True,
        prompt=None
    ).create(VENV_PATH)


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
    create_venv()


def entrypoint(usage_msg, main_func):
    """Generic entrypoint function. This sets up command line
    arguments for the invocation of a 'main' function and takes care
    of handling any vTDS exceptions that are raised to report
    errors. Other exceptions are allowed to pass to the caller for
    handling.

    """
    try:
        main_func(sys.argv[1:])
    except ContextualError as err:
        error_msg(str(err))
        sys.exit(1)
    except UsageError as err:
        usage(usage_msg, str(err))


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
