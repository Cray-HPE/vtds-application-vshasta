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
"""Private layer implementation module for the vshasta application.

"""
import os
from os.path import join as path_join
from yaml import safe_dump

from vtds_base import (
    ContextualError,
    info_msg
)
from vtds_base.layers.application import ApplicationAPI
from . import (
    APP_CONFIG_NAME,
    NODE_DEPLOY_SCRIPT_NAME,
    BLADE_DEPLOY_SCRIPT_NAME,
    COMMON_DEPLOY_LIB_NAME,
    script,
    home
)


class Application(ApplicationAPI):
    """PrivateApplication class, implements the vshasta application layer
    accessed through the python Application API.

    """

    def __init__(self, stack, config, build_dir):
        """Constructor, stash the root of the platfform tree and the
        digested and finalized application configuration provided by the
        caller that will drive all activities at all layers.

        """
        self.__doc__ = ApplicationAPI.__doc__
        self.config = config.get('application', None)
        if self.config is None:
            raise ContextualError(
                "no application configuration found in top level configuration"
            )
        self.stack = stack
        self.build_dir = build_dir
        self.app_config_path = path_join(self.build_dir, APP_CONFIG_NAME)
        self.prepared = False

    def __node_manifests(self):
        """Return the composed node manifests for deploying nodes.

        """
        pit_node_manifest = {
            'type': 'node',
            'class_names': ['pit_node'],
            'files': [
                (
                    script(NODE_DEPLOY_SCRIPT_NAME),
                    home(NODE_DEPLOY_SCRIPT_NAME),
                    'node-deploy'
                ),
                (
                    script(COMMON_DEPLOY_LIB_NAME),
                    home(COMMON_DEPLOY_LIB_NAME),
                    'common-deploy'
                ),
                (self.app_config_path, home(APP_CONFIG_NAME), 'config'),
            ],
            'script': path_join(os.sep, 'root', NODE_DEPLOY_SCRIPT_NAME),
        }
        virtual_blades = self.stack.get_provider_api().get_virtual_blades()
        blade_manifest = {
            'type': 'blade',
            'class_names': virtual_blades.blade_classes(),
            'files': [
                (
                    script(BLADE_DEPLOY_SCRIPT_NAME),
                    home(BLADE_DEPLOY_SCRIPT_NAME),
                    'node-deploy'
                ),
                (
                    script(COMMON_DEPLOY_LIB_NAME),
                    home(COMMON_DEPLOY_LIB_NAME),
                    'common-deploy'
                ),
                (self.app_config_path, home(APP_CONFIG_NAME), 'config'),
            ],
            'script': path_join(os.sep, 'root', BLADE_DEPLOY_SCRIPT_NAME),
        }
        return [
            pit_node_manifest,
            blade_manifest,
        ]

    def __make_host_ip_map(self, node_to_network_map):
        """Given a map of node_class to network_name mappings, compute
        a hostname to IP address map that reflects all of the hosts on
        each network.

        """
        virtual_nodes = self.stack.get_cluster_api().get_virtual_nodes()
        return {
            virtual_nodes.node_hostname(
                node_class, instance, network_name
            ): virtual_nodes.node_ipv4_addr(
                node_class, instance, network_name
            )
            for node_class, networks in node_to_network_map.items()
            for network_name in networks
            for instance in range(0, virtual_nodes.node_count(node_class))
            if virtual_nodes.node_ipv4_addr(
                node_class, instance, network_name
            ) is not None
        }

    @staticmethod
    def __deploy_manifest(connections, manifest, python_exe):
        """Copy files to the blades or nodes connected in
        'connections' based on the manifest and run the appropriate
        deployment script(s).

        """
        files = manifest['files']
        deploy_script = manifest['script']
        target_type = manifest['type']
        class_names = manifest['class_names']
        class_name_template = (
            "{{ node_class }} " if target_type == 'node'
            else "{{ blade_class }} "
        )
        for source, dest, tag in files:
            info_msg(
                "copying '%s' to Virtual %ss of types %s "
                "'%s'" % (
                    source, target_type.capitalize(), class_names, dest
                )
            )
            connections.copy_to(
                source, dest,
                recurse=False, logname="upload-application-%s-to-%s" % (
                    tag, target_type
                )
            )
        cmd = (
            "chmod 755 %s;" % deploy_script +
            "%s " % python_exe +
            "%s " % deploy_script +
            class_name_template +
            home(APP_CONFIG_NAME)
        )
        info_msg(
            "running '%s' on Virtual %ss of types %s" %
            (
                cmd, target_type.capitalize(), class_names
            )
        )
        connections.run_command(
            cmd, "run-%s-app-deploy-script-on" % target_type
        )

    def consolidate(self):
        return

    def prepare(self):
        virtual_nodes = self.stack.get_cluster_api().get_virtual_nodes()
        node_classes = virtual_nodes.node_classes()
        cluster_nets = {
            node_class: virtual_nodes.network_names(node_class)
            for node_class in node_classes
        }
        self.config['host_ipv4_map'] = self.__make_host_ip_map(cluster_nets)
        with open(self.app_config_path, 'w', encoding='UTF-8') as conf:
            safe_dump(self.config, stream=conf)
        self.prepared = True

    def validate(self):
        if not self.prepared:
            raise ContextualError(
                "cannot validate an unprepared application, "
                "call prepare() first"
            )

    def deploy(self):
        if not self.prepared:
            raise ContextualError(
                "cannot deploy an unprepared application, call prepare() first"
            )
        # Get the virtual nodes and virtual blades API objects for use
        # in deploying the manifests...
        virtual_nodes = self.stack.get_cluster_api().get_virtual_nodes()
        virtual_blades = self.stack.get_provider_api().get_virtual_blades()

        # Deploy the manifests to the virtual nodes and virtual blades.
        blade_py = self.stack.get_platform_api().get_blade_python_executable()
        for manifest in self.__node_manifests():
            class_names = manifest['class_names']
            node_or_blade = manifest['type']
            python_exe = "python3" if node_or_blade == 'node' else blade_py
            with (
                    virtual_nodes.ssh_connect_nodes(class_names)
                    if node_or_blade == 'node'
                    else virtual_blades.ssh_connect_blades(class_names)
            ) as connections:
                self.__deploy_manifest(connections, manifest, python_exe)

    def remove(self):
        if not self.prepared:
            raise ContextualError(
                "cannot deploy an unprepared application, call prepare() first"
            )
