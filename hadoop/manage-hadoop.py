#!/usr/bin/env python2

from __future__ import print_function
import argparse
import glob
import os.path
import re
import shutil
import tarfile
import tempfile
import urllib2

DEFAULT_FRAMEWORK_DIR="frameworks/"
DEFAULT_HADOOP_VERSION="2.6.0"
DEFAULT_YARN_MB=4096

HADOOP_VERSIONS={
    "2.6.0": {
        "url": "http://ftp.tudelft.nl/apache/hadoop/core/hadoop-2.6.0/hadoop-2.6.0.tar.gz",
        "archive_file": "hadoop-2.6.0.tar.gz",
        "root_dir": "hadoop-2.6.0",
        "template_dir": "2.6.x"
    }
}

class HadoopVersionNotSupportedError(Exception): pass
class DownloadFailedError(Exception): pass
class InstallFailedError(Exception): pass

def log(indentation, message):
    indent_str = ""
    while indentation > 1:
        indent_str += "|  "
        indentation -= 1
    if indentation == 1:
        indent_str += "|- "
    print(indent_str + message)

def parse_arguments():
    """Parses arguments passed on the command-line."""
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title="Hadoop deployment commands")

    def add_common_arguments(subparser):
        subparser.add_argument("-f", "--framework-dir", help="directory to store the Hadoop framework in", action="store", default=DEFAULT_FRAMEWORK_DIR)
        subparser.add_argument("-V", "--hadoop-version", metavar=("VERSION"), help="version of Hadoop to set up", action="store", default=DEFAULT_HADOOP_VERSION)

    # Add subparser for "install" command
    install_parser = subparsers.add_parser("install", help="install Hadoop", fromfile_prefix_chars="@")
    add_common_arguments(install_parser)
    install_parser.add_argument("--reinstall", help="force a clean download and installation of Hadoop", action="store_true")
    install_parser.add_argument("--list-hadoop-versions", help="list supported Hadoop versions", action="store_true")
    install_parser.set_defaults(func=install_hadoop)

    # Add subparser for "deploy" command
    deploy_parser = subparsers.add_parser("deploy", help="deploy Hadoop to a cluster", fromfile_prefix_chars="@")
    add_common_arguments(deploy_parser)
    cluster_nodes_group = deploy_parser.add_argument_group(title="Cluster nodes")
    cluster_nodes_group.add_argument("--master", help="Hadoop master node hostname", action="store", required=True)
    cluster_nodes_group.add_argument("--worker", help="Hadoop worker node hostname, multiple workers supported via repeated option or comma-separated list", action="append", default=[])
    hadoop_config_group = deploy_parser.add_argument_group(title="Hadoop configuration")
    hadoop_config_group.add_argument("--yarn-memory-mb", help="memory available per node to YARN in MB", action="store", default=DEFAULT_YARN_MB)
    hadoop_config_group.add_argument("--java-home", help="value of JAVA_HOME to deploy Hadoop with", action="store")
    deploy_parser.set_defaults(func=deploy_hadoop)

    # Add subparser for "list-hadoop-versions" command
    versions_parser = subparsers.add_parser("list-hadoop-versions", help="list supported Hadoop versions")
    versions_parser.set_defaults(func=list_hadoop_versions)

    return parser.parse_args()

def check_hadoop_version(requested_version):
    """Raises HadoopVersionNotSupportedError iff the given Hadoop version is not valid or not supported."""
    if not requested_version in HADOOP_VERSIONS:
        raise HadoopVersionNotSupportedError("Unsupported Hadoop version: %s." % requested_version)

def _fetch_hadoop(framework_dir, version, forced=False, tmpdir=None, indent=0):
    """Fetches and extracts a Hadoop distribution."""
    log(indent, "Obtaining Hadoop version %s distribution..." % version)

    # Check if a previous download of Hadoop already exists
    # If so, either remove for a forced reinstall, or skip fetching
    log(indent + 1, "Checking if Hadoop is present...")
    hadoop_target_dir = os.path.join(framework_dir, "hadoop-%s" % version)
    if os.path.exists(hadoop_target_dir):
        if forced:
            log(indent + 2, "Found previous installation of Hadoop. Removing to do a forced reinstall.")
            shutil.rmtree(hadoop_target_dir)
        else:
            log(indent + 2, "Found previous installation of Hadoop.")
            return
    else:
        log(indent + 2, "Found no previous installation of Hadoop.")

    # Repeat for the Hadoop archive
    log(indent + 1, "Checking if Hadoop archive is present...")
    hadoop_archive_target_dir = os.path.join(framework_dir, "archives")
    hadoop_archive_target_file = os.path.join(hadoop_archive_target_dir, HADOOP_VERSIONS[version]["archive_file"])
    if not os.path.exists(hadoop_archive_target_dir):
        try:
            os.makedirs(hadoop_archive_target_dir)
        except Exception as e:
            raise DownloadFailedError("Cannot create directory \"%s\" to store the Hadoop archive due to an unknown error: %s." % (hadoop_archive_target_dir, e))
    if os.path.exists(hadoop_archive_target_file):
        if not os.path.isfile(hadoop_archive_target_file):
            raise DownloadFailedError("Path \"%s\" already exists but is not a file. Failed to download Hadoop." % hadoop_archive_target_file)
        elif forced:
            log(indent + 2, "Found previously downloaded Hadoop archive. Removing to do a forced reinstall.")
            os.remove(hadoop_archive_target_file)
        else:
            log(indent + 2, "Found previously downloaded Hadoop archive. Skipping download.")
    else:
        log(indent + 2, "Hadoop archive not present.")

    # Download the Hadoop distribution if needed
    if not os.path.exists(hadoop_archive_target_file):
        hadoop_url = HADOOP_VERSIONS[version]["url"]
        try:
            with open(hadoop_archive_target_file, "wb") as archive_file:
                log(indent + 1, "Downloading Hadoop from \"%s\"..." % hadoop_url)
                download_stream = urllib2.urlopen(hadoop_url, timeout=1000)
                shutil.copyfileobj(download_stream, archive_file)
                log(indent + 2, "Download complete.")
        except urllib2.HTTPError as e:
            raise DownloadFailedError("Failed to download Hadoop from \"%s\" with HTTP status %d." % (hadoop_url, e.getcode()))
        except Exception as e:
            raise DownloadFailedError("Failed to download Hadoop from \"%s\" with unknown error: %s." % (hadoop_url, e))

    # Extract the Hadoop distribution to a temporary directory
    log(indent + 1, "Extracting Hadoop archive...")
    try:
        extract_tmp_dir = tempfile.mkdtemp(dir = tmpdir)
    except Exception as e:
        raise InstallFailedError("Failed to create temporary directory to extract Hadoop with unknown error: %s." % e)
    try:
        with tarfile.open(hadoop_archive_target_file) as hadoop_tar:
            hadoop_tar.extractall(extract_tmp_dir)
        log(indent + 2, "Extraction to temporary directory complete. Moving to framework directory...")
        shutil.move(os.path.join(extract_tmp_dir, HADOOP_VERSIONS[version]["root_dir"]), hadoop_target_dir)
        log(indent + 3, "Move complete.")
    except Exception as e:
        raise InstallFailedError("Failed to extract Hadoop archive \"%s\" with unknown error: %s." % (hadoop_archive_target_file, e))
    finally:
        shutil.rmtree(extract_tmp_dir)

    log(indent + 1, "Hadoop version %s is now available at \"%s\"." % (version, hadoop_target_dir))

def _deploy_hadoop(installation_dir, version, master, workers, yarn_mb, java_home, indent=0):
    """Deploys Hadoop to a given set of workers and a master node."""
    log(indent, "Deploying Hadoop to master \"%s\" and %d workers..." % (master, len(workers)))

    # Generate configuration files using the included templates
    template_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), HADOOP_VERSIONS[version]["template_dir"])
    config_dir = os.path.join(installation_dir, "etc", "hadoop")
    substitutions = {
        "__USER__": os.environ["USER"],
        "__MASTER__": master,
        "__YARN_MB__": str(yarn_mb)
    }
    if java_home:
        substitutions["${JAVA_HOME}"] = java_home
    substitutions_pattern = re.compile("|".join([re.escape(k) for k in substitutions.keys()]))
    # Iterate over template files and apply substitutions
    log(indent + 1, "Generating configuration files...")
    for template_file in glob.glob(os.path.join(template_dir, "*.template")):
        template_filename = os.path.basename(template_file)[:-len(".template")]
        log(indent + 2, "Generating file \"%s\"..." % template_filename)
        with open(template_file, "r") as template_in, open(os.path.join(config_dir, template_filename), "w") as config_out:
            for line in template_in:
                print(substitutions_pattern.sub(lambda m: substitutions[m.group(0)], line.rstrip()), file=config_out)
    log(indent + 2, "Generating file \"masters\"...")
    with open(os.path.join(config_dir, "masters"), "w") as masters_file:
        print(master, file=masters_file)
    log(indent + 2, "Generating file \"slaves\"...")
    with open(os.path.join(config_dir, "slaves"), "w") as slaves_file:
        for worker in workers:
            print(worker, file=slaves_file)
    log(indent + 2, "Configuration files generated.")

    # Clean up previous Hadoop deployments
    log(indent + 1, "Creating a clean environment on the master and workers...")
    local_hadoop_dir = "/local/%s/hadoop/" % substitutions["__USER__"]
    log(indent + 2, "Purging \"%s\" on master...")
    os.system('ssh "%s" "rm -rf \\"%s\\""' % (master, local_hadoop_dir))
    log(indent + 2, "Purging \"%s\" on workers...")
    for worker in workers:
        os.system('ssh "%s" "rm -rf \\"%s\\""' % (worker, local_hadoop_dir))
    log(indent + 2, "Creating directory structure on master...")
    os.system('ssh "%s" "mkdir -p \\"%s\\""' % (master, local_hadoop_dir))
    log(indent + 2, "Creating directory structure on workers...")
    for worker in workers:
        os.system('ssh "%s" "mkdir -p \\"%s/tmp\\" \\"%s/datanode\\""' % (worker, local_hadoop_dir, local_hadoop_dir))
    log(indent + 2, "Clean environment set up.")

    # Start HDFS
    log(indent + 1, "Deploying HDFS...")
    hadoop_home = os.path.realpath(installation_dir)
    log(indent + 2, "Formatting namenode...")
    os.system('ssh "%s" "\\"%s/bin/hadoop\\" namenode -format"' % (master, hadoop_home))
    log(indent + 2, "Starting HDFS...")
    os.system('ssh "%s" "\\"%s/sbin/start-dfs.sh\\""' % (master, hadoop_home))

    # Start YARN
    log(indent + 1, "Deploying YARN...")
    os.system('ssh "%s" "\\"%s/sbin/start-yarn.sh\\""' % (master, hadoop_home))

    log(indent + 1, "Hadoop cluster deployed.")

def install_hadoop(args):
    """Extracts arguments from CLI to install Hadoop."""
    # Verify that the Hadoop version is correct
    check_hadoop_version(args.hadoop_version)
    _fetch_hadoop(args.framework_dir, args.hadoop_version, args.reinstall)

def deploy_hadoop(args):
    """Extracts arguments from CLI to deploy Hadoop."""
    # Verify that the Hadoop version is correct
    check_hadoop_version(args.hadoop_version)
    # Verify that the Hadoop distribution is "installed"
    installation_dir = os.path.join(args.framework_dir, "hadoop-%s" % args.hadoop_version)
    if not os.path.isdir(installation_dir):
        print("Hadoop version %s not found at directory \"%s\". Please install Hadoop first." % (args.hadoop_version, installation_dir))
        return
    # Compose set of workers
    workers = set(worker.strip() for worker_list in args.worker if worker_list.strip() for worker in worker_list.split(",") if worker.strip())

    _deploy_hadoop(installation_dir, args.hadoop_version, args.master, workers, args.yarn_memory_mb, args.java_home)

def list_hadoop_versions(args):
    """Lists supported Hadoop versions."""
    print("Supported Hadoop versions:")
    for version in HADOOP_VERSIONS.keys():
        print("\t%s" % version)
    return

if __name__ == "__main__":
    args = parse_arguments()
    args.func(args)

