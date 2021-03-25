#!/usr/bin/env python2

from __future__ import print_function

from ..package import PackageRegistry, get_package_registry
from ..condapackage import CondaPackage, CondaPackageVersion
from .. import util

import fnmatch
import glob
import os
import re

_SETTING_WEBSERVER_PORT = "webserver_port"
_ALL_SETTINGS = [
    (_SETTING_WEBSERVER_PORT, "port the webserver will listen on")
]

_DEFAULT_WEBSERVER_PORT = 10800

class AirflowPackageVersion(CondaPackageVersion):
    def __init__(self, version, conda_packages = [], conda_channels = [], pip_packages = [], template_dir = ""):
        super(AirflowPackageVersion, self).__init__(version, conda_packages, conda_channels, pip_packages)
        self.__template_dir = template_dir

    @property
    def template_dir(self):
        return self.__template_dir

class AirflowPackage(CondaPackage):
    def __init__(self):
        super(AirflowPackage, self).__init__("airflow", "Airflow")

    def deploy_installed(self, conda_env, package_version, machines, settings, log_fn=util.log):
        """Deploys Airflow to a given master node."""
        if len(machines) < 1:
            raise util.InvalidSetupError("Airflow requires at least one machine to run on.")

        # Extract settings
        webserver_port = settings.pop(_SETTING_WEBSERVER_PORT, _DEFAULT_WEBSERVER_PORT)
        if len(settings) > 0:
            raise util.InvalidSetupError("Found unknown settings for Airflow: '%s'" % "','".join(settings.keys()))

        # Select master node to run Airflow on
        master = machines[0]
        log_fn(0, "Deploying Airflow on machine \"%s\"..." % master)

        # Define directories for Airflow files (data, metadata, config files)
        airflow_home = "/local/%s/airflow" % os.environ["USER"]
        airflow_dag_dir = os.path.abspath(os.path.join(conda_env.root, "var", "airflow", "dags"))

        # Clean up previous Airflow deployments
        log_fn(1, "Removing old environment on the Airflow machine...")
        util.execute_command_quietly(["ssh", master, 'rm -rf "%s"' % airflow_home])
        util.execute_command_quietly(["ssh", master, 'rm -rf "%s"' % airflow_dag_dir])
        log_fn(2, "Old environment removed.")

        # Generate configuration files using the included templates
        log_fn(1, "Generating configuration files...")
        # - Find template files
        template_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), "conf", "airflow", package_version.template_dir))
        template_files = []
        for template_subdir, _, filenames in os.walk(template_dir):
            for filename in fnmatch.filter(filenames, "*.template"):
                template_files.append(os.path.join(os.path.relpath(template_subdir, template_dir), filename))
        # - Generate a list of variables to substitute
        substitutions = {
            "__USER__": os.environ["USER"],
            "__HOST__": master,
            "__CONDA_ROOT__": conda_env.root,
            "__AIRFLOW_HOME__": airflow_home,
            "__AIRFLOW_DAGS__": airflow_dag_dir
        }
        substitutions_pattern = re.compile("|".join([re.escape(k) for k in substitutions.keys()]))
        # - Iterate over template files and apply substitutions
        for rel_template_file_src in template_files:
            rel_template_file_dst = rel_template_file_src[:-len(".template")]
            template_file_src = os.path.join(template_dir, rel_template_file_src)
            template_file_dst = os.path.join(airflow_home, rel_template_file_dst)
            log_fn(2, "Generating file \"<airflow_home>/%s\"..." % rel_template_file_dst)
            file_content = []
            with open(template_file_src, "r") as template_in:
                for line in template_in:
                    file_content.append(substitutions_pattern.sub(lambda m: substitutions[m.group(0)], line.rstrip()))
            util.write_remote_file(master, template_file_dst, "\n".join(file_content), os.stat(template_file_src).st_mode & 0o777)
        log_fn(2, "Configuration files generated.")

        # Create PostgreSQL user and database
        log_fn(1, "Creating PostgreSQL user and database for Airflow...")
        conda_env.remote_command(master, ["createuser", "airflow"])
        conda_env.remote_command(master, ["createdb", "--owner=airflow", "airflow"])
        log_fn(2, "PostgreSQL integration initialized.")

        # Initialize Airflow
        log_fn(1, "Initializing Airflow...")
        conda_env.remote_command(master, ["AIRFLOW_HOME=\"%s\"" % airflow_home, "airflow", "db", "init"])
        conda_env.remote_command(master, ["AIRFLOW_HOME=\"%s\"" % airflow_home, "airflow", "users", "create",
          "-u", os.environ["USER"], "-p", os.environ["USER"], "-f", "Default", "-l", "User", "-r", "Admin", "-e", "%s@localhost" % os.environ["USER"]])
        util.execute_command_quietly(["ssh", master, 'mkdir -p "%s"' % airflow_dag_dir])
        log_fn(2, "Airflow database initialized.")

        # Start Airflow
        log_fn(1, "Starting Airflow daemons...")
        conda_env.remote_command(master, ["AIRFLOW_HOME=\"%s\"" % airflow_home, "airflow", "webserver", "-H", master, "-p", str(webserver_port), "-D"])
        conda_env.remote_command(master, ["AIRFLOW_HOME=\"%s\"" % airflow_home, "airflow", "scheduler", "-D"])

        log_fn(1, 'Airflow is now listening on "%s:%s".' % (master, webserver_port))

    def get_supported_deployment_settings(self, framework_version):
        return _ALL_SETTINGS

get_package_registry().register_package(AirflowPackage())
get_package_registry().package("airflow").add_version(AirflowPackageVersion("2.0.1", conda_packages=["sqlalchemy=1.3.23", "psycopg2=2.8.6"], pip_packages=["apache-airflow==2.0.1"], template_dir="2.x"))
