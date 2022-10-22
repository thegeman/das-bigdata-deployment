#!/usr/bin/env python2

from . import util
from .package import Package, PackageVersion

import os.path
import shutil
import tarfile
import tempfile
import urllib2

class NativePackage(Package):
    def __init__(self, identifier, name):
        super(NativePackage, self).__init__(identifier, name)

    def deploy(self, package_dir, package_version, reservation_id, machines, settings, log_fn=util.log):
        _try_download_native_package(package_dir, self, package_version, log_fn=log_fn)
        _try_install_native_package(package_dir, self, package_version, log_fn=log_fn)
        self.deploy_installed(_install_dir(package_dir, self, package_version), package_version, machines, settings, log_fn=log_fn)

    def deploy_installed(self, install_dir, package_version, machines, settings, log_fn=util.log):
        raise NotImplementedError()

    def __repr__(self):
        return "NativePackage{identifier=%s,name=%s}" % (self.identifier, self.name)


class NativePackageVersion(PackageVersion):
    def __init__(self, version, archive_url, archive_extension, archive_root_dir):
        super(NativePackageVersion, self).__init__(version)
        self.__archive_url = archive_url
        self.__archive_extension = archive_extension.lstrip('.')
        self.__archive_root_dir = archive_root_dir

    @property
    def archive_url(self):
        return self.__archive_url

    @property
    def archive_extension(self):
        return self.__archive_extension

    @property
    def archive_root_dir(self):
        return self.__archive_root_dir

    def __repr__(self):
        return self.version

def _archive_dir(package_dir):
    return os.path.join(package_dir, "archives")

def _archive_file(package_dir, package, package_version):
    return os.path.join(_archive_dir(package_dir), "%s.%s" % (package.version_identifier(package_version.version), package_version.archive_extension))

def _check_if_archive_present(package_dir, package, package_version):
    """Checks if an archive is already present."""
    archive_file = _archive_file(package_dir, package, package_version)
    return os.path.exists(archive_file) and os.path.isfile(archive_file)

def _install_dir(package_dir, package, package_version):
    return os.path.join(package_dir, package.version_identifier(package_version.version))

def _try_download_native_package(package_dir, package, package_version, log_fn=util.log):
    """Fetches a Big Data package distribution."""
    log_fn(0, "Obtaining %s version %s distribution..." % (package.name, package_version.version))

    # Check if a previous download of the package already exists
    # If so, either remove for a forced redownload, or complete
    log_fn(1, "Checking if archive for %s version %s is present..." % (package.name, package_version.version))
    archive_file = _archive_file(package_dir, package, package_version)
    if _check_if_archive_present(package_dir, package, package_version):
        log_fn(2, "Found previously downloaded %s archive. Skipping download." % package.name)
        return
    else:
        log_fn(2, "%s archive not present." % package.name)
        if not os.path.exists(_archive_dir(package_dir)):
            try:
                os.makedirs(_archive_dir(package_dir))
            except Exception as e:
                raise DownloadFailedError("Cannot create directory \"%s\" to store the %s archive due to an unknown error: %s." % (_archive_dir(package_dir), package.name, e))

    # Download the package distribution
    dist_url = package_version.archive_url
    log_fn(1, "Downloading %s version %s from \"%s\"..." % (package.name, package_version.version, dist_url))
    try:
        with open(archive_file, "wb") as archive_stream:
            download_stream = urllib2.urlopen(dist_url, timeout=1000)
            shutil.copyfileobj(download_stream, archive_stream)
            log_fn(2, "Download complete.")
    except urllib2.HTTPError as e:
        raise DownloadFailedError("Failed to download %s from \"%s\" with HTTP status %d." % (package.name, dist_url, e.getcode()))
    except Exception as e:
        raise DownloadFailedError("Failed to download %s from \"%s\" with unknown error: %s." % (package.name, dist_url, e))

def _try_install_native_package(package_dir, package, package_version, log_fn=util.log):
    """Installs a Big Data package distribution."""
    log_fn(0, "Installing %s version %s..." % (package.name, package_version.version))

    # Check if a previous installation of the package already exists
    # If so, either remove for a forced reinstall, or return
    log_fn(1, "Checking if previous installation of %s version %s is present..." % (package.name, package_version.version))
    target_dir = _install_dir(package_dir, package, package_version)
    if os.path.exists(target_dir):
        log_fn(2, "Found previous installation of %s." % package.name)
        return
    else:
        log_fn(2, "Found no previous installation of %s." % package.name)

    # Check if the archive file is already present
    if not _check_if_archive_present(package_dir, package, package_version):
        raise MissingArchiveError("Archive for %s version %s is not present in \"%s\"." % (package.name, version, _archive_dir(package_dir)))

    # Extract the distribution to a temporary directory
    log_fn(1, "Extracting %s version %s archive..." % (package.name, package_version.version))
    try:
        extract_tmp_dir = tempfile.mkdtemp()
    except Exception as e:
        raise InstallFailedError("Failed to create temporary directory to extract %s with unknown error: %s." % (package.name, e))
    try:
        with tarfile.open(_archive_file(package_dir, package, package_version)) as archive_tar:
            def is_within_directory(directory, target):
                
                abs_directory = os.path.abspath(directory)
                abs_target = os.path.abspath(target)
            
                prefix = os.path.commonprefix([abs_directory, abs_target])
                
                return prefix == abs_directory
            
            def safe_extract(tar, path=".", members=None, *, numeric_owner=False):
            
                for member in tar.getmembers():
                    member_path = os.path.join(path, member.name)
                    if not is_within_directory(path, member_path):
                        raise Exception("Attempted Path Traversal in Tar File")
            
                tar.extractall(path, members, numeric_owner=numeric_owner) 
                
            
            safe_extract(archive_tar, extract_tmp_dir)
        log_fn(2, "Extraction to temporary directory complete. Moving to package directory...")
        shutil.move(os.path.join(extract_tmp_dir, package_version.archive_root_dir), target_dir)
        log_fn(3, "Move complete.")
    except Exception as e:
        raise InstallFailedError("Failed to extract %s archive \"%s\" with unknown error: %s." % (package.name, _archive_file(package_dir, package, package_version), e))
    finally:
        shutil.rmtree(extract_tmp_dir)

    log_fn(1, "%s version %s is now available at \"%s\"." % (package.name, package_version.version, target_dir))
