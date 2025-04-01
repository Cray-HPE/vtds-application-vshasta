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
from ipaddress import IPv4Network
import os
from os.path import join as path_join
import re
from uuid import uuid4
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


# pylint: disable=too-many-instance-attributes
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
        self.virt_blades = None  # Cached VirtualBlades API object
        self.virt_nodes = None  # Cached VirtualNodes API object
        self.virt_nets = None  # Cached VirtualNetworks API object
        self.nets_by_role = None  # Cached map of role to network name
        self.hosted_node_map = None  # Cached map blade-class to node-classes
        self.blade_xnames = None  # Cached blade XNAME mappings
        self.prepared = False

    def __virtual_nodes(self, refresh=False):
        """Get and cache the current VirtualNodes API object from the
        Cluster Layer. Generally these don't change, so caching them
        is valid. There are operations in the vShasta Application that
        do update information on Virtual Nodes. Those operations
        should call this function again with the refresh flag set to
        'True' after completing the changes and before returning. That
        will ensure that the cached objects are kept up to date.

        """
        self.virt_nodes = (
            self.virt_nodes if not refresh and self.virt_nodes is not None
            else self.stack.get_cluster_api().get_virtual_nodes()
        )
        return self.virt_nodes

    def __virtual_networks(self, refresh=False):
        """Get and cache the current VirtualNetworks API object from the
        Cluster Layer. Generally these don't change, so caching them
        is valid. There are operations in the vShasta Application that
        do update information on Virtual Networks. Those operations
        should call this function again with the refresh flag set to
        'True' after completing the changes and before returning. That
        will ensure that the cached objects are kept up to date.

        """
        self.virt_nets = (
            self.virt_nets if not refresh and self.virt_nets is not None
            else self.stack.get_cluster_api().get_virtual_networks()
        )
        return self.virt_nets

    def __virtual_blades(self, refresh=False):
        """Get and cache the current VirtualBlades API object from the
        Cluster Layer. Generally these don't change, so caching them
        is valid. There could be operations in the vShasta Application
        that update information on Virtual Blades. Those operations
        should call this function again with the refresh flag set to
        'True' after completing the changes and before returning. That
        will ensure that the cached objects are kept up to date.

        """
        self.virt_blades = (
            self.virt_blades if not refresh and self.virt_blades is not None
            else self.stack.get_provider_api().get_virtual_blades()
        )
        return self.virt_blades

    def __hosted_node_map(self):
        """Get the mapping of Virtual Blade classes to their
        respective hosted node classes. This will be a dictionary
        indexed by Virtual Blade class name of lists of Virtual Node
        class names.

        """
        if self.hosted_node_map is not None:
            return self.hosted_node_map
        # Don't have a cached map, so build one and return it.
        virtual_nodes = self.__virtual_nodes()
        node_classes = virtual_nodes.node_classes()
        node_hosts = {
            node_class: virtual_nodes.node_host_blade_info(
                node_class
            )['blade_class']
            for node_class in node_classes
        }
        # Again, use a set comprehension to make a list of blade
        # classes that actually host nodes from the node_hosts list.
        blade_classes = {
            node_hosts[node_class]
            for node_class in node_classes
        }
        # Now build the map...
        self.hosted_node_map = {
            blade_class: [
                node_class
                for node_class in node_classes
                if blade_class == node_hosts[node_class]
            ]
            for blade_class in blade_classes
        }
        return self.hosted_node_map

    def __node_role_string(self, node_class):
        """Return a role:subrole string encoding of the application
        metadata node role information for the specified node class if
        that node class has one and it is not None. Otherwise return
        None.

        """
        virtual_nodes = self.__virtual_nodes()
        return virtual_nodes.application_metadata(node_class).get(
            'node_role', None
        )

    def __hosted_nodes(self, blade_class, blade_instance):
        """Return a dictionary indexed by node_class of node instance
        lists hosted by the specified instance of the specified blade
        class.

        """
        virtual_nodes = self.__virtual_nodes()
        host_map = self.__hosted_node_map()
        node_classes = host_map[blade_class]
        # Need node roles to filter out nodes that have no roles in
        # vShasta. The node this particularly has in mind is the PIT
        # node, but it applies to any node class in the vTDS cluster
        # that is not part of the vShasta system (allowing a virtual
        # data center with a vShasta in it to be modeled to some
        # degree).
        node_roles = {
            node_class: self.__node_role_string(node_class)
            for node_class in node_classes
            if self.__node_role_string(node_class) is not None
        }
        capacities = {
            node_class: virtual_nodes.node_host_blade_info(
                node_class
            )['instance_capacity']
            for node_class in node_classes
        }
        return {
            node_class: list(
                range(
                    blade_instance * capacities[node_class],
                    (blade_instance + 1) * capacities[node_class]
                )
            )
            for node_class in node_classes
            if node_class in node_roles
        }

    def __first_blade_slot(self, blade_class_list, blade_class):
        """Given a list of blade classes and the name of a blade class
        in that list, calculate "slot number" of the first blade
        instance of the blade class assuming that all blades are
        packed in-order by class into numbered slots.

        """
        virtual_blades = self.__virtual_blades()
        slot = 0
        for item in blade_class_list:
            if item == blade_class:
                return slot
            slot += virtual_blades.get_blade_count(item)
        # If we got here, we never found 'blade_class' in the
        # list. This is some kind of programming error.
        raise ContextualError(
            "(internal error) unable to find blade class name '%s' "
            "in blade class list %s" % (blade_class, blade_class_list)
        )

    def __blade_xnames(self):
        """Retrieve the mapping of blades to XNAMEs. The mapping is a
        dictionary whose keys are Virtual Blade class name / instance
        number tuples (must be tuples, not lists) and whose values are
        blade XNAMEs as strings.

        """
        if self.blade_xnames is not None:
            return self.blade_xnames
        river = (
            self.config
            .get('geometry', {})
            .get('cabinets', {})
            .get('river', {})
        )
        cabinets = river.get('blade_classes', None)
        if cabinets is None:
            raise ContextualError(
                "Application Layer can't find chassis blade class contents "
                "configuration while preparing xnames"
            )
        virtual_blades = self.__virtual_blades()
        self.blade_xnames = {
            (blade_class, blade_instance): "x%dc%ds%db0" % (
                int(cabinet),
                int(chassis),
                self.__first_blade_slot(blade_classes, blade_class) +
                blade_instance
            )
            for cabinet, chassis_list in cabinets.items()
            for chassis, blade_classes in chassis_list.items()
            for blade_class in blade_classes
            for blade_instance in range(
                0, virtual_blades.get_blade_count(blade_class)
            )
        }
        return self.blade_xnames

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
        virtual_blades = self.__virtual_blades()
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
        virtual_nodes = self.__virtual_nodes()
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

    def __assign_node_xnames(self):
        """Using the configured geometry as a guide, determine the
        xnames for the virtual blades and virtual nodes and assign the
        xnames to the virtual nodes as Virtual Node names.

        """
        blade_xnames = self.__blade_xnames()
        node_xnames = {
            (node_class, node_instance): "%sn%d" % (blade_xname, node_index)
            for node_index, node_instance in enumerate(node_instances)
            for node_class, node_instances in self.__hosted_nodes(
                    blade_class, blade_instance
            ).items()
            for (blade_class, blade_instance), blade_xname in
            blade_xnames.items()
        }
        # Now that we know the xnames, push them into the Cluster
        # configuration for future use. This will cause the Virtual
        # Node implementation to be able to address Virtual Nodes by
        # xname for things like RedFish.
        virtual_nodes = self.__virtual_nodes()
        for (node_class, node_instance), node_xname in node_xnames:
            virtual_nodes.set_node_hostnamme(
                node_class, node_instance, node_xname
            )
        # The Cluster config for virtual nodes has been altered by
        # this so we need to refresh self.virt_nodes.
        self.__virtual_nodes(refresh=True)

    def __make_xname_map(self, node_classes):
        """Create a dictionary mapping xnames to nodes (node_class /
        instance) xnames. Prerequisite is that __assign_node_xnames()
        has been run.

        """
        virtual_nodes = self.__virtual_nodes()
        return {
            virtual_nodes.node_node_name(node_class, instance):
            (node_class, instance)
            for instance in range(0, virtual_nodes.node_count(node_class))
            for node_class in node_classes
        }

    def __make_app_node_config(self):
        """Collect and construct the data needed to build the
        'application_node_config.yaml' seed file for CSI. This will be
        used on the PIT node to generate the seed files and run 'csi
        config init' during deployment.

        """
        return {}  # IMPLEMENT THIS! ERIC

    def __make_cabinets(self):
        """Collect and construct the data needed to build the
        'cabinets.yaml' seed file for CSI. This will be used on the
        PIT node to generate the seed files and run 'csi config init'
        during deployment.

        """
        return {}  # IMPLEMENT THIS! ERIC

    def __make_hmn_connections(self):
        """Collect and construct the data needed to build the
        'hsm_connections.json' seed file for CSI. This will be used on
        the PIT node to generate the seed files and run 'csi config
        init' during deployment.

        """
        return {}  # IMPLEMENT THIS! ERIC

    def __make_ncn_metadata(self):
        """Collect and construct the data needed to build the
        'ncn_metadata.csv' seed file for CSI. This will be used on the
        PIT node to generate the seed files and run 'csi config init'
        during deployment.

        """
        return {}  # IMPLEMENT THIS! ERIC

    def __make_switch_metadata(self):
        """Collect and construct the data needed to build the
        'switch_metadata.csv' seed file for CSI. This will be used on
        the PIT node to generate the seed files and run 'csi config
        init' during deployment.

        """
        return {}  # IMPLEMENT THIS! ERIC

    def __net_role(self, network_name):
        """Get the network role, if any, from the application
        metadata in the configuration for a given network name.

        """
        virtual_nets = self.__virtual_networks()
        return virtual_nets.application_metadata(network_name).get(
            'network_role', None
        )

    def __list_management_hostnames(self):
        """Return a list of management hostnames (all nodes with a
        management role).

        """
        return []  # IMPLEMENT THIS! ERIC

    def __net_by_role(self, role):
        """Return a mapping of network roles to network names based on
        Virtual Network metadata.

        """
        virtual_nets = self.__virtual_networks()
        self.nets_by_role = (
            self.nets_by_role if self.nets_by_role is not None
            else {
                    self.__net_role(network_name): network_name
                    for network_name in virtual_nets.network_names()
                    if self.__net_role(network_name) is not None
            }
        )
        return self.nets_by_role(role)

    def __get_network_cidr(self, role):
        """Retrieve the CIDR from the Virtual Network with the
        designated role matching the specified role.

        """
        virtual_nets = self.__virtual_networks()
        network_name = self.__net_by_role(role)
        return virtual_nets.ipv4_cidr(network_name)

    def __validate_cidr(self, cidr, net_cidr, prefix_only=False):
        """Check that a specified CIDR ('cidr') or IP address (if
        'prefix_only' is True) falls within a specified Network CIDR
        ('net_cidr'). Raise a Contextual Error if the validation
        fails.

        """
        if prefix_only:
            # For prefix only, remove any netmask part that might be
            # there, then append '/32' to make it a host CIDR.
            cidr = cidr[:cidr.find('/')] + '/32'
        sub_net = IPv4Network(cidr, strict=False)
        net = IPv4Network(net_cidr)
        return sub_net.subnet_of(net)

    def __csm_version(self):
        """Return the CSM version from the Application Layer
        configuration parsed into a tuple containing 5 parts:

        - Major Number
        - Minor Number
        - Patch Number
        - Label
        - Build string

        Where the expected form of the CSM version is a string of the
        following form: <major>.<minor>.<patch>-<label>+<build>. Where
        label and build are optional.

        """
        csm_version = self.config.get('csm', {}).get('version', None)
        if csm_version is None:
            raise ContextualError(
                "no CSM version (application.csm.version) provided in the "
                "Application Layer configuration"
            )
        version_re = re.compile(
            r"""
            ^
            (?P<major>\d+)  # major
            [.]
            (?P<minor>\d+)  # minor
            [.]
            (?P<patch>\d+)  # patch
            (?:-(?P<label>[0-9A-Za-z\-\.]+))?   # optional label
            (?:[+](?P<build>[0-9A-Za-z\-\.]+))?  # optional build
            $
            """,
            re.VERBOSE
        )
        version_match = version_re.match(csm_version)
        if version_match is None:
            raise ContextualError(
                "CSM version (application.csm.version) supplied '%s' "
                "is not a valid semantic version" % csm_version
            )
        return (
            version_match.group('major'),
            version_match.group('minor'),
            version_match.group('patch'),
            version_match.group('label'),
            version_match.group('build'),
        )

    def __get_site_dns(self):
        """Get the first of the DNS servers from the Provider supplied
        site DNS configuration.

        """
        site_config = self.stack.get_provider_api().get_site_config()
        dns = site_config.site_dns_servers()
        return (
            dns[0]['address']
            if dns
            and 'address' in dns[0]
            and dns[0]['address'] is not None
            else ""
        )

    def __get_site_ntp(self):
        """Get the first NTP server from the Provider supplied
        site NTP configuration.

        """
        site_config = self.stack.get_provider_api().get_site_config()
        servers = site_config.site_ntp_servers()
        return [
            server['hostname']
            if server['hostname'] is not None
            else server['address']
            for server in servers if server['hostname'] or server['address']
        ]

    def __make_system_config(self):
        """Collect and construct the data needed to build the
        'system_config.yaml' seed file for CSI. This will be used on
        the PIT node to generate the seed files and run 'csi config
        init' during deployment.

        """
        sys_config = (
            self.config.get('seed_files', {}).get('system_config', None)
        )
        if sys_config is None:
            raise ContextualError(
                "application layer configuration contains no 'system_config' "
                "seed file settings"
            )
        # Set up the CIDR information for all of the networks in the
        # system config.
        sys_config['can-cidr'] = self.__get_network_cidr('CAN')
        sys_config['chn-cidr'] = self.__get_network_cidr('CHN')
        sys_config['cmn-cidr'] = self.__get_network_cidr('CMN')
        sys_config['hmn-cidr'] = self.__get_network_cidr('HMN')
        sys_config['hsn-cidr'] = self.__get_network_cidr('HSN')
        sys_config['nmn-cidr'] = self.__get_network_cidr('NMN')

        # While the *-gw fields in the system configuration seem to be
        # deprecated in favor of *-gateway, there may be versions of
        # CSM for which this is not true, and we don't want to break
        # them. Make sure that the deprecated fields are essentially
        # aliases of the new fields.
        sys_config['can-gw'] = sys_config['can-gateway']
        sys_config['cmn-gw'] = sys_config['cmn-gateway']

        # Grab the 'major.minor' CSM version from the application
        # configuration.
        sys_config['csm-version'] = ".".join(self.__csm_version()[0:2])

        # vShasta only supports River Cabinets and the settings for
        # those are found in the Application Layer 'geometry'
        # section. Because of the way I do Virtual Node naming in the
        # Cluster Layer, there is no support for a starting NID number
        # other than 1, so hard-code that here.
        river = (
            self.config
            .get('geometry', {})
            .get('cabinets', {})
            .get('river', {})
        )
        sys_config['river-cabinets'] = str(river.get('count', '1'))
        sys_config['starting-river-cabinet'] = str(
            river.get(
                'starting_id', "3000"
            )
        )
        sys_config['starting-river-nid'] = "1"

        # Create a random password for the BMCs. Users should never
        # need to know this, so it is fine just to keep it here.
        bmc = self.config.get('bmc', {})
        if not bmc:
            raise ContextualError(
                "prerequisite failure: BMC configuration in Application "
                "Layer configuration has not been set up yet."
            )
        sys_config['bootstrap-ncn-bmc-user'] = bmc['bmc_user']
        sys_config['bootstrap-ncn-bmc-pass'] = bmc['bmc_passwd']

        # The following is the default setting for this currently
        # unused item. If it becomes used in the future it will be a
        # setting gotten from the Cluster Layer as part of the NCN
        # interface definitions.
        sys_config['install-ncn-bond-members'] = "p1p1,p10p1"

        # All of the management (Master, Worker and Storage) nodes are
        # NTP peers in the cluster.
        sys_config['ntp-peers'] = self.__list_management_hostnames()
        sys_config['site-dns'] = self.__get_site_dns()
        sys_config['ipv4-resolvers'] = self.__get_site_dns()
        site_config = self.stack.get_provider_api().get_site_config()
        sys_config['system-name'] = site_config.system_name()
        sys_config['upstream-ntp-server'] = self.__get_site_ntp()

    def __validate_system_config(self):
        """Validate the System Configuration seed file contents in the
        Application Layer configuration.

        """
        sys_config = (
            self.config.get('seed_files', {}).get('system_config', None)
        )
        if sys_config is None:
            raise ContextualError(
                "application layer configuration contains no 'system_config' "
                "seed file settings"
            )

        # Validate the dynamic and static address pools for all of the
        # networks whose dynamic and static pools are required to be
        # within the CIDR of the network. The NMN is special in this
        # regard because its dynamic and static pools are taken from
        # Kubernetes endpoints that exist in an overlay of the NMN.
        self.__validate_cidr(
            sys_config.get('hmn-dynamic-pool', ""),
            sys_config['hmn-cidr']
        )
        self.__validate_cidr(
            sys_config.get('hmn-static-pool', ""),
            sys_config['hmn-cidr']
        )
        self.__validate_cidr(
            sys_config.get('hsn-dynamic-pool', ""),
            sys_config['hsn-cidr']
        )
        self.__validate_cidr(
            sys_config.get('hsn-static-pool', ""),
            sys_config['hsn-cidr']
        )
        self.__validate_cidr(
            sys_config.get('can-dynamic-pool', ""),
            sys_config['can-cidr']
        )
        self.__validate_cidr(
            sys_config.get('can-static-pool', ""),
            sys_config['can-cidr']
        )

        # Make sure network gateways are within their respective
        # networks on the CAN and CMN.
        self.__validate_cidr(
            sys_config.get('can-gateway', ""),
            sys_config['can-cidr'],
            prefix_only=True
        )
        self.__validate_cidr(
            sys_config.get('cmn-gateway', ""),
            sys_config['cmn-cidr'],
            prefix_only=True
        )

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
            connections.run_command(
                "chmod 0600 %s" % dest, "restrict-access-to-%s-on-%s" % (
                    tag, target_type
                )
            )
        cmd = (
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
        virtual_nodes = self.__virtual_nodes()
        node_classes = virtual_nodes.node_classes()
        cluster_nets = {
            node_class: virtual_nodes.network_names(node_class)
            for node_class in node_classes
        }
        bmc = self.config.get('bmc', {})
        bmc['bmc_user'] = bmc['bmc_user'] if 'bmc_user' in bmc else 'root'
        bmc['bmc_passwd'] = str(uuid4())
        self.__assign_node_xnames()
        self.config['host_ipv4_map'] = self.__make_host_ip_map(cluster_nets)
        self.config['xname_map'] = self.__make_xname_map(node_classes)
        self.__make_app_node_config()
        self.__make_cabinets()
        self.__make_hmn_connections()
        self.__make_ncn_metadata()
        self.__make_switch_metadata()
        self.__make_system_config()

    def prepare(self):
        with open(self.app_config_path, 'w', encoding='UTF-8') as conf:
            safe_dump(self.config, stream=conf)
        self.prepared = True

    def validate(self):
        if not self.prepared:
            raise ContextualError(
                "cannot validate an unprepared application, "
                "call prepare() first"
            )
        self.__validate_system_config()

    def deploy(self):
        if not self.prepared:
            raise ContextualError(
                "cannot deploy an unprepared application, call prepare() first"
            )
        # Get the virtual nodes and virtual blades API objects for use
        # in deploying the manifests...
        virtual_nodes = self.__virtual_nodes()
        virtual_blades = self.__virtual_blades()

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
