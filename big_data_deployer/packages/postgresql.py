#!/usr/bin/env python2

from __future__ import print_function

from ..package import PackageRegistry, get_package_registry
from ..condapackage import CondaPackage, CondaPackageVersion
from .. import util

import fnmatch
import glob
import os
import re

_ALL_SETTINGS = [
]

class PostgreSQLPackageVersion(CondaPackageVersion):
    def __init__(self, version, conda_packages = [], conda_channels = [], pip_packages = [], template_dir = ""):
        super(PostgreSQLPackageVersion, self).__init__(version, conda_packages, conda_channels, pip_packages)
        self.__template_dir = template_dir

    @property
    def template_dir(self):
        return self.__template_dir

class PostgreSQLPackage(CondaPackage):
    def __init__(self):
        super(PostgreSQLPackage, self).__init__("postgresql", "PostgreSQL")

    def deploy_installed(self, conda_env, package_version, machines, settings, log_fn=util.log):
        """Deploys PostgreSQL to a given master node."""
        if len(machines) < 1:
            raise util.InvalidSetupError("PostgreSQL requires at least one machine to run on.")

        # Extract settings
        if len(settings) > 0:
            raise util.InvalidSetupError("Found unknown settings for PostgreSQL: '%s'" % "','".join(settings.keys()))

        # Select master node to run PostgreSQL on
        master = machines[0]
        log_fn(0, "Deploying PostgreSQL on machine \"%s\"..." % master)

        # Define root directory of PostgreSQL files (data, metadata, config files)
        postgresql_data_root = "/local/%s/postgresql" % os.environ["USER"]

        # Clean up previous PostgreSQL deployments
        log_fn(1, "Removing old environment on the PostgreSQL machine...")
        util.execute_command_quietly(["ssh", master, 'rm -rf "%s"' % postgresql_data_root])
        log_fn(2, "Old environment removed.")

        # Create empty database
        log_fn(1, "Initializing PostgreSQL database...")
        conda_env.remote_command(master, ["initdb", "-D", postgresql_data_root])
        log_fn(2, "Database initialized.")

        # Generate configuration files using the included templates
        log_fn(1, "Generating configuration files...")
        # - Find template files
        template_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), "conf", "postgresql", package_version.template_dir))
        template_files = []
        for template_subdir, _, filenames in os.walk(template_dir):
            for filename in fnmatch.filter(filenames, "*.template"):
                template_files.append(os.path.join(os.path.relpath(template_subdir, template_dir), filename))
        # - Generate a list of variables to substitute
        substitutions = {
            "__USER__": os.environ["USER"],
            "__HOST__": master,
            "__CONDA_ROOT__": conda_env.root,
            "__DATA_DIR__": postgresql_data_root
        }
        substitutions_pattern = re.compile("|".join([re.escape(k) for k in substitutions.keys()]))
        # - Iterate over template files and apply substitutions
        for rel_template_file_src in template_files:
            rel_template_file_dst = rel_template_file_src[:-len(".template")]
            template_file_src = os.path.join(template_dir, rel_template_file_src)
            template_file_dst = os.path.join(postgresql_data_root, rel_template_file_dst)
            log_fn(2, "Generating file \"<postgresql_root>/%s\"..." % rel_template_file_dst)
            file_content = []
            with open(template_file_src, "r") as template_in:
                for line in template_in:
                    file_content.append(substitutions_pattern.sub(lambda m: substitutions[m.group(0)], line.rstrip()))
            util.write_remote_file(master, template_file_dst, "\n".join(file_content), os.stat(template_file_src).st_mode & 0o777)
        log_fn(2, "Configuration files generated.")

        # Ensure that /var/log exists in the Conda environment
        log_dir = os.path.join(conda_env.root, "var", "log")
        if not os.path.exists(log_dir):
           os.makedirs(log_dir)

        # Start PostgreSQL
        log_fn(1, "Starting PostgreSQL daemon...")
        conda_env.remote_command(master, ["pg_ctl", "-D", postgresql_data_root, "-l", os.path.join(log_dir, "postgres"), "start"])

        log_fn(1, 'PostgreSQL is now listening on "%s:5432".' % master)

    def get_supported_deployment_settings(self, framework_version):
        return _ALL_SETTINGS

get_package_registry().register_package(PostgreSQLPackage())
get_package_registry().package("postgresql").add_version(PostgreSQLPackageVersion("12.2", conda_packages=["postgresql=12.2"], conda_channels=[], template_dir="12.x"))

