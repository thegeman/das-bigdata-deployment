#!/usr/bin/env python2

from __future__ import print_function
from . import *
import argparse
import sys

DEFAULT_FRAMEWORK_DIR="frameworks/"

def parse_arguments():
    parser = argparse.ArgumentParser(description="Install and deploy Big Data frameworks", prog="big_data_deployer")
    subparsers = parser.add_subparsers(title="Big Data framework deployment commands")

    add_list_frameworks_subparser(subparsers)
    add_install_subparser(subparsers)

    return parser.parse_args()

def add_list_frameworks_subparser(parser):
    list_frameworks_parser = parser.add_parser("list-frameworks", help="list supported Big Data frameworks")
    list_frameworks_parser.add_argument("--versions", help="list all supported versions", action="store_true")
    list_frameworks_parser.set_defaults(func=list_frameworks)

def add_install_subparser(parser):
    install_parser = parser.add_parser("install", help="install a Big Data framework")
    install_parser.add_argument("-f", "--framework-dir", help="directory to store the Big Data framework in", action="store", default=DEFAULT_FRAMEWORK_DIR)
    install_parser.add_argument("--reinstall", help="force a clean reinstallation of the framework", action="store_true")
    install_parser.add_argument("FRAMEWORK", help="name of the framework to install", action="store")
    install_parser.add_argument("VERSION", help="version of the framework to install", action="store")
    install_parser.set_defaults(func=install_framework)

def list_frameworks(args):
    print("Supported frameworks:")
    if args.versions:
        for framework_ident, framework in sorted(get_framework_registry().frameworks.iteritems()):
            for version in sorted(framework.versions):
                print("%s %s" % (framework_ident, version))
    else:
        for framework_ident in sorted(get_framework_registry().frameworks):
            print(framework_ident)

def install_framework(args):
    fm = FrameworkManager(get_framework_registry(), args.framework_dir)
    fm.install(args.FRAMEWORK, args.VERSION, force_reinstall=args.reinstall)

def main():
    args = parse_arguments()
    args.func(args)

if __name__ == "__main__":
    main()

