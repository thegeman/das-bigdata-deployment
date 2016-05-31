#!/usr/bin/env python2

from .frameworkmanager import Framework, FrameworkVersion, FrameworkRegistry, get_framework_registry

class ZookeeperFrameworkVersion(FrameworkVersion):
    def __init__(self, version, archive_url, archive_extension, archive_root_dir, template_dir):
        super(ZookeeperFrameworkVersion, self).__init__(version, archive_url, archive_extension, archive_root_dir)
        self.__template_dir = template_dir

    @property
    def template_dir(self):
        return self.__template_dir

get_framework_registry().register_framework(Framework("zookeeper", "ZooKeeper"))
get_framework_registry().framework("zookeeper").add_version(ZookeeperFrameworkVersion("3.4.8", "http://ftp.tudelft.nl/apache/zookeeper/zookeeper-3.4.8/zookeeper-3.4.8.tar.gz", "tar.gz", "zookeeper-3.4.8", "3.4.x"))
