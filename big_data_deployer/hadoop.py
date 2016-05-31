#!/usr/bin/env python2

from .frameworkmanager import Framework, FrameworkVersion, FrameworkRegistry, get_framework_registry

class HadoopFrameworkVersion(FrameworkVersion):
    def __init__(self, version, archive_url, archive_extension, archive_root_dir, template_dir):
        super(HadoopFrameworkVersion, self).__init__(version, archive_url, archive_extension, archive_root_dir)
        self.__template_dir = template_dir

    @property
    def template_dir(self):
        return self.__template_dir

get_framework_registry().register_framework(Framework("hadoop", "Hadoop"))
get_framework_registry().framework("hadoop").add_version(HadoopFrameworkVersion("2.6.0", "http://ftp.tudelft.nl/apache/hadoop/core/hadoop-2.6.0/hadoop-2.6.0.tar.gz", "tar.gz", "hadoop-2.6.0", "2.6.x"))
