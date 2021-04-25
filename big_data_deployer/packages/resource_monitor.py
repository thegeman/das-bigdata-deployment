#!/usr/bin/env python2

from __future__ import print_function

from ..package import PackageRegistry, get_package_registry
from ..nativepackage import NativePackage, NativePackageVersion
from .. import util

import glob
import os.path
import re

class ResourceMonitorPackageVersion(NativePackageVersion):
    def __init__(self, version, archive_url, archive_extension, archive_root_dir, requires_make):
        super(ResourceMonitorPackageVersion, self).__init__(version, archive_url, archive_extension, archive_root_dir)
        self.__requires_make = requires_make

    @property
    def requires_make(self):
        return self.__requires_make

class ResourceMonitorPackage(NativePackage):
    def __init__(self):
        super(ResourceMonitorPackage, self).__init__("resource-monitor", "Resource Monitor")

    def deploy_installed(self, resource_monitor_home, package_version, machines, settings, log_fn=util.log):
        """Deploys a resource monitor on every node in a cluster."""
        if len(machines) < 1:
            raise util.InvalidSetupError("Resource Monitor requires at least one machine to run on.")

        # Ensure that RESOURCE_MONITOR_HOME is an absolute path
        resource_monitor_home = os.path.realpath(resource_monitor_home)

        # Resource Monitor currently has no settings
        if len(settings) > 0:
            raise util.InvalidSetupError("Found unknown settings for Resource Monitor: '%s'" % "','".join(settings.keys()))

        # Build the resource monitor binary if needed
        if package_version.requires_make:
            log_fn(1, "Compiling Resource Monitor binaries...")
            #util.execute_command_quietly(['bash', '-c', '"module load cuda10.1/toolkit; cd %s; make"' % resource_monitor_home], shell=True)
            old_dir = os.getcwd()
            os.chdir(resource_monitor_home)
            try:
                util.execute_command_quietly(['module load cuda10.1/toolkit; make'], shell=True)
            finally:
                os.chdir(old_dir)

        # Clean up previous resource monitor deployments
        log_fn(1, "Creating a clean environment on each machine...")
        local_resource_monitor_dir = "/local/%s/resource-monitor/" % os.environ["USER"]
        log_fn(2, "Purging \"%s\" on machines..." % local_resource_monitor_dir)
        for machine in machines:
            util.execute_command_quietly(['ssh', machine, 'rm -rf "%s"' % local_resource_monitor_dir])
        log_fn(2, "Creating directory structure on machines...")
        for machine in machines:
            util.execute_command_quietly(['ssh', machine, 'mkdir -p "%s/metrics" "%s/logs"' % \
                (local_resource_monitor_dir, local_resource_monitor_dir)])
        log_fn(2, "Clean environment set up.")

        # Start the resource monitor daemon on every machine
        log_fn(1, "Deploying Resource Monitor to every machine in the reservation...")
        for machine in machines:
            log_fn(2, "Starting monitoring daemon on %s..." % machine)
            util.execute_command_verbose(['ssh', machine,
                '%s/bin/resource-monitor' % resource_monitor_home,
                '-D',
                '-p %s/resource-monitor.pid' % local_resource_monitor_dir,
                '-l %s/logs/resource-monitor-$(hostname).log' % local_resource_monitor_dir,
                '-o %s/metrics/' % local_resource_monitor_dir])
        log_fn(1, "Resource Monitor is now running on all machines.")

    def get_supported_deployment_settings(self, package_version):
        return []

get_package_registry().register_package(ResourceMonitorPackage())
get_package_registry().package('resource-monitor').add_version(ResourceMonitorPackageVersion(
    version = '0.2',
    archive_url = 'https://github.com/thegeman/resource-monitor/archive/refs/tags/v0.2.tar.gz',
    archive_extension = '.tar.gz',
    archive_root_dir = 'resource-monitor-0.2',
    requires_make = True
))

