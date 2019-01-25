#!/usr/bin/env python2

from __future__ import print_function
from .frameworkmanager import Framework, FrameworkVersion, FrameworkRegistry, get_framework_registry
from . import util
import fnmatch
import glob
import os
import re

class InfluxDBFrameworkVersion(FrameworkVersion):
    def __init__(self, version, archive_url, archive_extension, archive_root_dir, template_dir):
        super(InfluxDBFrameworkVersion, self).__init__(version, archive_url, archive_extension, archive_root_dir)
        self.__template_dir = template_dir

    @property
    def template_dir(self):
        return self.__template_dir

class InfluxDBFramework(Framework):
    def __init__(self):
        super(InfluxDBFramework, self).__init__("influxdb", "InfluxDB")

    def deploy(self, influxdb_home, framework_version, machines, settings, log_fn=util.log):
        """Deploys InfluxDB to a given master node."""
        if len(machines) < 1:
            raise util.InvalidSetupError("InfluxDB requires at least one machine to run on.")

        # Extract settings -- InfluxDB currently has no settings
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
        template_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "conf", "influxdb", framework_version.template_dir))
        template_files = []
        for template_subdir, _, filenames in os.walk(template_dir):
            for filename in fnmatch.filter(filenames, "*.template"):
                template_files.append(os.path.join(os.path.relpath(template_subdir, template_dir), filename))
        # - Generate a list of variables to substitute
        substitutions = {
            "__USER__": os.environ["USER"],
            "__HOST__": master,
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

        log_fn(1, 'InfluxDB is now listening on "%s:8086".' % master)

    def get_supported_deployment_settings(self, framework_version):
        return []

get_framework_registry().register_framework(InfluxDBFramework())
get_framework_registry().framework("influxdb").add_version(InfluxDBFrameworkVersion("1.7.3", "https://dl.influxdata.com/influxdb/releases/influxdb-1.7.3_linux_amd64.tar.gz", "tar.gz", "influxdb-1.7.3-1", "1.7.x"))
