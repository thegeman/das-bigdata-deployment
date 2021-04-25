#!/usr/bin/env python2

from __future__ import print_function

from ..package import PackageRegistry, get_package_registry
from ..nativepackage import NativePackage, NativePackageVersion
from .. import util

import fnmatch
import glob
import os.path
import re

class ResourceMonitorPackageVersion(NativePackageVersion):
    def __init__(self, version, archive_url, archive_extension, archive_root_dir, template_dir, requires_make):
        super(ResourceMonitorPackageVersion, self).__init__(version, archive_url, archive_extension, archive_root_dir)
        self.__template_dir = template_dir
        self.__requires_make = requires_make

    @property
    def template_dir(self):
        return self.__template_dir

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

        # Generate configuration files using the included templates
        log_fn(1, "Generating configuration files...")
        # - Find template files
        template_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), "conf", "resource-monitor", package_version.template_dir))
        template_files = []
        for template_subdir, _, filenames in os.walk(template_dir):
            for filename in fnmatch.filter(filenames, "*.template"):
                template_files.append(os.path.join(os.path.relpath(template_subdir, template_dir), filename))
        # - Generate a list of variables to substitute
        substitutions = {
            "__USER__": os.environ["USER"],
            "__MACHINES__": " ".join(machines)
        }
        substitutions_pattern = re.compile("|".join([re.escape(k) for k in substitutions.keys()]))
        # - Iterate over template files and apply substitutions
        for rel_template_file_src in template_files:
            rel_template_file_dst = rel_template_file_src[:-len(".template")]
            template_file_src = os.path.join(template_dir, rel_template_file_src)
            template_file_dst = os.path.join(resource_monitor_home, rel_template_file_dst)
            log_fn(2, "Generating file \"<resource_monitor_home>/%s\"..." % rel_template_file_dst)
            parent_dir = os.path.dirname(template_file_dst)
            if not os.path.exists(parent_dir):
                os.makedirs(parent_dir)
            with open(template_file_src, "r") as template_in, open(template_file_dst, "w") as config_out:
                for line in template_in:
                    print(substitutions_pattern.sub(lambda m: substitutions[m.group(0)], line.rstrip()), file=config_out)
            os.chmod(template_file_dst, os.stat(template_file_src).st_mode)
        log_fn(2, "Configuration files generated.")

        # Build the resource monitor binary if needed
        if package_version.requires_make:
            log_fn(1, "Compiling Resource Monitor binaries...")
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
        util.execute_command_quietly(['%s/sbin/start-all.sh' % resource_monitor_home])
        log_fn(1, "Resource Monitor is now running on all machines.")

    def get_supported_deployment_settings(self, package_version):
        return []

get_package_registry().register_package(ResourceMonitorPackage())
get_package_registry().package('resource-monitor').add_version(ResourceMonitorPackageVersion(
    version = '0.3',
    archive_url = 'https://github.com/thegeman/resource-monitor/archive/refs/tags/v0.3.tar.gz',
    archive_extension = '.tar.gz',
    archive_root_dir = 'resource-monitor-0.3',
    template_dir = '0.3',
    requires_make = True
))

