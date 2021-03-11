#!/usr/bin/env python2

from __future__ import print_function

from ..package import PackageRegistry, get_package_registry
from ..nativepackage import NativePackage, NativePackageVersion
from .. import util

import fnmatch
import glob
import os
import re

_SETTING_PORT = "port"
_SETTING_ZOOKEEPER_URL = "zookeeper_url"
_ALL_SETTINGS = [
    (_SETTING_PORT, "port to bind Kafka to"),
    (_SETTING_ZOOKEEPER_URL, "URL of Zookeeper instance to connect to")
]

_DEFAULT_PORT = 9092
_DEFAULT_ZOOKEEPER_URL = "127.0.0.1:2181"

class KafkaPackageVersion(NativePackageVersion):
    def __init__(self, version, archive_url, archive_extension, archive_root_dir, template_dir):
        super(KafkaPackageVersion, self).__init__(version, archive_url, archive_extension, archive_root_dir)
        self.__template_dir = template_dir

    @property
    def template_dir(self):
        return self.__template_dir

class KafkaPackage(NativePackage):
    def __init__(self):
        super(KafkaPackage, self).__init__("kafka", "Kafka")

    def deploy_installed(self, kafka_home, package_version, machines, settings, log_fn=util.log):
        """Deploys Kafka to a given master node."""
        if len(machines) < 1:
            raise util.InvalidSetupError("Kafka requires at least one machine to run on.")

        # Extract settings
        port = settings.pop(_SETTING_PORT, _DEFAULT_PORT)
        zookeeper_url = settings.pop(_SETTING_ZOOKEEPER_URL, _DEFAULT_ZOOKEEPER_URL)
        if len(settings) > 0:
            raise util.InvalidSetupError("Found unknown settings for Kafka: '%s'" % "','".join(settings.keys()))

        # Select master node to run Kafka on
        master = machines[0]
        log_fn(0, "Selected Kafka machine \"%s\"." % master)

        # Ensure that KAFKA_HOME is an absolute path
        kafka_home = os.path.realpath(kafka_home)

        # Generate configuration files using the included templates
        log_fn(1, "Generating configuration files...")
        # - Find template files
        template_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), "conf", "kafka", package_version.template_dir))
        template_files = []
        for template_subdir, _, filenames in os.walk(template_dir):
            for filename in fnmatch.filter(filenames, "*.template"):
                template_files.append(os.path.join(os.path.relpath(template_subdir, template_dir), filename))
        # - Generate a list of variables to substitute
        substitutions = {
            "__USER__": os.environ["USER"],
            "__HOST__": master,
            "__HOME_DIR__": kafka_home,
            "__DATA_DIR__": "/local/%s/kafka" % os.environ["USER"],
            "__PORT__": str(port),
            "__ZOOKEEPER_URL__": zookeeper_url
        }
        substitutions_pattern = re.compile("|".join([re.escape(k) for k in substitutions.keys()]))
        # - Iterate over template files and apply substitutions
        for rel_template_file_src in template_files:
            rel_template_file_dst = rel_template_file_src[:-len(".template")]
            template_file_src = os.path.join(template_dir, rel_template_file_src)
            template_file_dst = os.path.join(kafka_home, rel_template_file_dst)
            log_fn(2, "Generating file \"<kafka_home>/%s\"..." % rel_template_file_dst)
            parent_dir = os.path.dirname(template_file_dst)
            if not os.path.exists(parent_dir):
                os.makedirs(parent_dir)
            with open(template_file_src, "r") as template_in, open(template_file_dst, "w") as config_out:
                for line in template_in:
                    print(substitutions_pattern.sub(lambda m: substitutions[m.group(0)], line.rstrip()), file=config_out)
            os.chmod(template_file_dst, os.stat(template_file_src).st_mode)
        log_fn(2, "Configuration files generated.")

        # Clean up previous Kafka deployments
        log_fn(1, "Creating a clean environment on the Kafka machine...")
        local_kafka_dir = "/local/%s/kafka/" % substitutions["__USER__"]
        log_fn(2, "Purging \"%s\"..." % local_kafka_dir)
        util.execute_command_quietly(["ssh", master, 'rm -rf "%s"' % local_kafka_dir])
        log_fn(2, "Creating directory structure...")
        util.execute_command_quietly(['ssh', master, 'mkdir -p "%s"' % local_kafka_dir])
        log_fn(2, "Clean environment set up.")

        # Start Kafka
        log_fn(1, "Starting Kafka broker...")
        util.execute_command_quietly(['ssh', master, '"%s/bin/kafka-server-start.sh"' % kafka_home, '-daemon', '"%s/config/server.properties"' % kafka_home])

        log_fn(1, 'Kafka is now listening on "%s:%s".' % (master, port))

    def get_supported_deployment_settings(self, package_version):
        return _ALL_SETTINGS

get_package_registry().register_package(KafkaPackage())
get_package_registry().package("kafka").add_version(KafkaPackageVersion("2.13-2.7.0", "https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=/kafka/2.7.0/kafka_2.13-2.7.0.tgz", "tar.gz", "kafka_2.13-2.7.0", "2.7.x"))

