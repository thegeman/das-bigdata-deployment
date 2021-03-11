#!/usr/bin/env python2

from __future__ import print_function

from ..package import PackageRegistry, get_package_registry
from ..nativepackage import NativePackage, NativePackageVersion
from .. import util

import fnmatch
import glob
import os
import re

_SETTING_HTTP_PORT = "http_port"
_SETTING_RPC_PORT = "rpc_port"
_ALL_SETTINGS = [
    (_SETTING_HTTP_PORT, "port to bind InfluxDB HTTP interface to"),
    (_SETTING_RPC_PORT, "port to bind InfluxDB RPC interface to")
]

_DEFAULT_HTTP_PORT = 8086
_DEFAULT_RPC_PORT = 8088

class InfluxDBPackageVersion(NativePackageVersion):
    def __init__(self, version, archive_url, archive_extension, archive_root_dir, template_dir):
        super(InfluxDBPackageVersion, self).__init__(version, archive_url, archive_extension, archive_root_dir)
        self.__template_dir = template_dir

    @property
    def template_dir(self):
        return self.__template_dir

class InfluxDBPackage(NativePackage):
    def __init__(self):
        super(InfluxDBPackage, self).__init__("influxdb", "InfluxDB")

    def deploy_installed(self, influxdb_home, package_version, machines, settings, log_fn=util.log):
        """Deploys InfluxDB to a given master node."""
        if len(machines) < 1:
            raise util.InvalidSetupError("InfluxDB requires at least one machine to run on.")

        # Extract settings
        http_port = settings.pop(_SETTING_HTTP_PORT, _DEFAULT_HTTP_PORT)
        rpc_port = settings.pop(_SETTING_RPC_PORT, _DEFAULT_RPC_PORT)
        if len(settings) > 0:
            raise util.InvalidSetupError("Found unknown settings for InfluxDB: '%s'" % "','".join(settings.keys()))

        # Select master node to run InfluxDB on
        master = machines[0]
        log_fn(0, "Selected InfluxDB machine \"%s\"." % master)

        # Ensure that INFLUXDB_HOME is an absolute path
        influxdb_home = os.path.realpath(influxdb_home)

        # Generate configuration files using the included templates
        log_fn(1, "Generating configuration files...")
        # - Find template files
        template_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), "conf", "influxdb", package_version.template_dir))
        template_files = []
        for template_subdir, _, filenames in os.walk(template_dir):
            for filename in fnmatch.filter(filenames, "*.template"):
                template_files.append(os.path.join(os.path.relpath(template_subdir, template_dir), filename))
        # - Generate a list of variables to substitute
        substitutions = {
            "__USER__": os.environ["USER"],
            "__HOST__": master,
            "__HTTP_PORT__": str(http_port),
            "__RPC_PORT__": str(rpc_port),
            "__HOME_DIR__": influxdb_home,
            "__DATA_DIR__": "/local/%s/influxdb" % os.environ["USER"]
        }
        substitutions_pattern = re.compile("|".join([re.escape(k) for k in substitutions.keys()]))
        # - Iterate over template files and apply substitutions
        for rel_template_file_src in template_files:
            rel_template_file_dst = rel_template_file_src[:-len(".template")]
            template_file_src = os.path.join(template_dir, rel_template_file_src)
            template_file_dst = os.path.join(influxdb_home, rel_template_file_dst)
            log_fn(2, "Generating file \"<influxdb_home>/%s\"..." % rel_template_file_dst)
            parent_dir = os.path.dirname(template_file_dst)
            if not os.path.exists(parent_dir):
                os.makedirs(parent_dir)
            with open(template_file_src, "r") as template_in, open(template_file_dst, "w") as config_out:
                for line in template_in:
                    print(substitutions_pattern.sub(lambda m: substitutions[m.group(0)], line.rstrip()), file=config_out)
            os.chmod(template_file_dst, os.stat(template_file_src).st_mode)
        log_fn(2, "Configuration files generated.")

        # Clean up previous InfluxDB deployments
        log_fn(1, "Creating a clean environment on the InfluxDB machine...")
        local_influxdb_dir = "/local/%s/influxdb/" % substitutions["__USER__"]
        log_fn(2, "Purging \"%s\"..." % local_influxdb_dir)
        util.execute_command_quietly(["ssh", master, 'rm -rf "%s"' % local_influxdb_dir])
        log_fn(2, "Creating directory structure...")
        util.execute_command_quietly(['ssh', master, 'mkdir -p "%s"' % local_influxdb_dir])
        log_fn(2, "Clean environment set up.")

        # Start InfluxDB
        log_fn(1, "Starting InfluxDB daemon...")
        util.execute_command_quietly(['ssh', master, '"%s/sbin/start-influxdb"' % influxdb_home])

        log_fn(1, 'InfluxDB is now listening on "%s:%s" (HTTP) and "%s:%s" (RPC).' % (master, http_port, master, rpc_port))

    def get_supported_deployment_settings(self, package_version):
        return _ALL_SETTINGS

get_package_registry().register_package(InfluxDBPackage())
get_package_registry().package("influxdb").add_version(InfluxDBPackageVersion("1.7.3", "https://dl.influxdata.com/influxdb/releases/influxdb-1.7.3_linux_amd64.tar.gz", "tar.gz", "influxdb-1.7.3-1", "1.7.x"))
