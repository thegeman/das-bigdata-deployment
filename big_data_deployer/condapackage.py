#!/usr/bin/env python2

from . import util
from .package import Package, PackageVersion
from conda import get_conda_env

import os.path
import shutil
import tarfile
import tempfile
import urllib2

class CondaPackage(Package):
    def __init__(self, identifier, name):
        super(CondaPackage, self).__init__(identifier, name)

    def deploy(self, package_dir, package_version, reservation_id, machines, settings, log_fn=util.log):
        conda_env = self.__get_or_create_conda_env(package_dir, reservation_id, log_fn=log_fn)
        self.__install_conda_package(conda_env, self, package_version, log_fn=log_fn)
        self.deploy_installed(conda_env, package_version, machines, settings, log_fn=log_fn)

    def deploy_installed(self, conda, package_version, machines, settings, log_fn=util.log):
        raise NotImplementedError()

    def __repr__(self):
        return "CondaPackage{identifier=%s,name=%s}" % (self.identifier, self.name)

    def __get_or_create_conda_env(self, package_dir, reservation_id, log_fn=util.log):
        log_fn(0, "Looking for Conda environment...")

        # Get a handle to the Conda environment
        conda_env = get_conda_env(reservation_id, package_dir)

        # Return if the environment already exists
        if conda_env.exists():
            log_fn(1, "Found existing environment at '%s'." % conda_env.root)
            return conda_env

        # Otherwise, create a new Conda environment
        log_fn(1, "Creating new Conda environment at '%s'..." % conda_env.root)
        conda_env.create()
        log_fn(2, "Conda environment succesfully created.")
        return conda_env

    def  __install_conda_package(self, conda_env, package, package_version, log_fn=util.log):
        log_fn(0, "Installing Conda package and dependencies for %s version %s..." % (package.name, package_version.version))
        if package_version.conda_packages:
            conda_env.install(package_version.conda_packages, package_version.conda_channels)
        if package_version.pip_packages:
            conda_env.pip_install(package_version.pip_packages)
        log_fn(1, "Installation completed.")

class CondaPackageVersion(PackageVersion):
    def __init__(self, version, conda_packages=[], conda_channels=[], pip_packages=[]):
        super(CondaPackageVersion, self).__init__(version)
        self.__conda_packages = [conda_packages] if isinstance(conda_packages, str) else conda_packages
        self.__conda_channels = [conda_channels] if isinstance(conda_channels, str) else conda_channels
        self.__pip_packages = [pip_packages] if isinstance(pip_packages, str) else pip_packages

    @property
    def conda_packages(self):
        return self.__conda_packages

    @property
    def conda_channels(self):
        return self.__conda_channels

    @property
    def pip_packages(self):
        return self.__pip_packages

    def __repr__(self):
        return self.version
