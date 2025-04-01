#!python3
#
# MIT License
#
# (C) Copyright 2025 Hewlett Packard Enterprise Development LP
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
#
# pylint: disable=consider-using-f-string,too-many-locals
"""
Compose initial SLS data using the data from the terraform vshasta.yaml
configuration and produce an SLS input file that can be stored in GCS by
terraform.
"""
import os
import json
import yaml

from .deploy_application_common import ContextualError


def read_yaml_file(path):
    """Read in a YAML file and return the resulting data structure
    with appropriate error handling.
    """
    try:
        with open(path, 'r', encoding="UTF-8") as data:
            content = yaml.safe_load(data)
    except OSError as err:
        raise ContextualError(str(err)) from err
    except yaml.error.YAMLError as err:
        raise ContextualError(
            "failed to parse '%s' as YAML: %s" % (path, err)
        ) from err
    return content


def write_yaml_file(path, yaml_data):
    """Given a python dictionary describing the full SLS configuration
    for a vShasta system, dump out the JSON data describing that system.

    """
    try:
        with open(path, 'w', encoding="UTF-8") as output:
            yaml.dump(yaml_data, output)
    except OSError as err:
        raise ContextualError(str(err)) from err
    except yaml.YAMLError as err:
        raise ContextualError(
            "Failed to format '%s' as YAML: %s" % (path, err)
        ) from err


def write_json_file(path, json_data):
    """Given a python dictionary write out a named file containing the
    JSON encoded data.

    """
    try:
        with open(path, 'w', encoding="UTF-8") as output:
            json.dump(json_data, output, indent=2)
    except OSError as err:
        raise ContextualError(str(err)) from err


def write_csv_file(path, csv_data):
    """Given a list of CSV strings write out a named file containing
    the data.

    """
    try:
        with open(path, 'w', encoding="UTF-8") as output:
            for line in csv_data:
                output.write(line + "\n")
    except OSError as err:
        raise ContextualError(
            "failed to write '%s' as CSV: %s" % (path, str(err))
        ) from err


def generate_seed_files(config, build_dir):
    """Generate all of the necessary seed files to run `csi config
    init` on the PIT node based on the vShasta application
    configuration data.

    """
    # Generate the data we are going to put in the seed files
    seed_files = config.get('seed_files', {})
    system_config = seed_files.get('system_config', {})
    hmn_connections = seed_files.get('hmn_connections', [])
    ncn_metadata = seed_files.get('ncn_metadata', [])
    switch_metadata = seed_files.get('switch_metadata', [])
    application_node_config = seed_files.get('application_node_config', {})
    cabinets = seed_files.get('cabinets', {})

    # Write out the seed files...
    write_yaml_file(
        os.path.join(build_dir, "system_config.yaml"),
        system_config
    )
    write_yaml_file(
        os.path.join(build_dir, "cabinets.yaml"),
        cabinets
    )
    write_yaml_file(
        os.path.join(build_dir, "application_node_config.yaml"),
        application_node_config
    )
    write_json_file(
        os.path.join(build_dir, "hmn_connections.json"),
        hmn_connections
    )
    write_csv_file(
        os.path.join(build_dir, "ncn_metadata.csv"),
        ncn_metadata
    )
    write_csv_file(
        os.path.join(build_dir, "switch_metadata.csv"),
        switch_metadata
    )
