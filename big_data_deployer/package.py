#!/usr/bin/env python2

from . import util

class DownloadFailedError(Exception): pass
class MissingArchiveError(Exception): pass
class InstallFailedError(Exception): pass

class Package(object):
    def __init__(self, identifier, name):
        self.__identifier = identifier
        self.__name = name
        self.__versions = {}

    @property
    def identifier(self):
        return self.__identifier

    @property
    def name(self):
        return self.__name

    @property
    def versions(self):
        return self.__versions.copy()

    def version(self, version_no):
        if version_no in self.__versions:
            return self.__versions[version_no]
        else:
            raise KeyError("Version %s of %s has not been registered." % (version_no, self.__name))

    def version_identifier(self, version_no):
        return "%s-%s" % (self.__identifier, version_no)

    def add_version(self, package_version):
        self.__versions[package_version.version] = package_version

    def deploy(self, package_version, reservation_id, machines, settings, log_fn=util.log):
        raise NotImplementedError()

    def get_supported_deployment_settings(self, package_version):
        return []

    def __repr__(self):
        return "Package{identifier=%s,name=%s}" % (self.identifier, self.name)

class PackageVersion(object):
    def __init__(self, version):
        self.__version = version

    @property
    def version(self):
        return self.__version

    def __repr__(self):
        return self.version

class PackageRegistry:
    def __init__(self):
        self.__packages = {}

    def register_package(self, package):
        self.__packages[package.identifier] = package

    @property
    def packages(self):
        return self.__packages.copy()

    def package(self, package_identifier):
        if package_identifier in self.__packages:
            return self.__packages[package_identifier]
        else:
            raise KeyError("Package %s has not been registered." % package_identifier)

__PackageRegistry_singleton = PackageRegistry()
def get_package_registry():
    return __PackageRegistry_singleton

class PackageManager:
    def __init__(self, package_registry, package_dir):
        self.__package_dir = package_dir
        self.__package_registry = package_registry

    @property
    def package_registry(self):
        return self.__package_registry

    @property
    def package_dir(self):
        return self.__package_dir

    def deploy(self, package_identifier, version, reservation_id, machines, settings, log_fn=util.log):
        """Deploys a Big Data package distribution."""
        package = self.package_registry.package(package_identifier)
        package_version = package.version(version)
        log_fn(0, "Deploying %s version %s to cluster of %d machine(s)..." % (package.name, version, len(machines)))

        package.deploy(self.package_dir, package_version, reservation_id, machines, settings, log_fn=util.create_log_fn(1, log_fn))

    def get_supported_deployment_settings(self, package_identifier, version):
        """Retrieves a list of supported deployment settings and their descriptions for a given Big Data package and version."""
        package = self.package_registry.package(package_identifier)
        package_version = package.version(version)
        return package.get_supported_deployment_settings(package_version)
