#!/usr/bin/env python2

from __future__ import print_function

from . import util
from . import preserve

import argparse
import os
import pipes
import sys
import time

class CondaEnv:
    def __init__(self, preserve_id, framework_dir):
        self.__root = self.__get_conda_root_for_reservation(preserve_id, framework_dir)

    @property
    def root(self):
        return self.__root

    def exists(self):
        return os.path.isdir(self.root)

    def __ensure_exists(self):
        if not self.exists():
            raise Error("Conda environment has not yet been created at '%s'." % self.root)

    def command(self, command_line, verbose=False):
        if not isinstance(command_line, str):
            command_line = " ".join([pipes.quote(arg) for arg in command_line])
        command_line = "conda activate %s && %s" % (pipes.quote(self.root), command_line)
        command_line = "bash --login -c %s" % pipes.quote(command_line)
        util.execute_command(command_line, verbose=verbose, shell=True)

    def remote_command(self, machine, command_line, verbose=False):
        if not isinstance(command_line, str):
            command_line = " ".join([pipes.quote(arg) for arg in command_line])
        command_line = "ssh %s conda activate %s \"&&\" %s" % (machine, pipes.quote(self.root), command_line)
        util.execute_command(command_line, verbose=verbose, shell=True)

    def create(self, python_version=None, pip_version=None, verbose=False):
        if os.path.exists(self.root):
            raise Error("Cannot create Conda environment. Path '%s' already exists." % self.root)
        command_line = ["conda", "create", "-y", "--prefix", self.root,
            "python=%s" % python_version if python_version else "python",
            "pip=%s" % pip_version if pip_version else "pip"]
        util.execute_command(command_line, verbose=verbose)

    def install(self, packages, channels=[], verbose=False):
        command_line = ["conda", "install", "-y"]
        for channel in channels:
            command_line.append("--channel")
            command_line.append(channel)
        command_line.extend(packages)
        self.command(command_line, verbose=verbose)

    def pip_install(self, packages, verbose=False):
        command_line = ["pip", "install"]
        command_line.extend(packages)
        self.command(command_line, verbose=verbose)

    def __get_conda_root_for_reservation(self, preserve_id, framework_dir):
        reservation = preserve.get_PreserveManager().fetch_reservation(preserve_id)
        resolved_id = reservation.reservation_id

        return os.path.realpath(os.path.join(framework_dir, "conda-%s" % str(resolved_id)))

def get_conda_env(preserve_id, framework_dir):
    return CondaEnv(preserve_id, framework_dir)

def add_conda_subparser(parser):
    conda_parser = parser.add_parser("conda", help="set up and configure a Conda environment")
    conda_parser.add_argument("-f", "--framework-dir", help="installation directory for Big Data frameworks", action="store", default=util.DEFAULT_FRAMEWORK_DIR)
    conda_parser.add_argument("--preserve-id", help="preserve reservation id to use for deployment, or 'LAST' for the last reservation made by the user", action="store", default="LAST")
    conda_subparsers = conda_parser.add_subparsers(title="Conda environment management commands")

    # Add subparser for "create" command
    conda_create_parser = conda_subparsers.add_parser("create", help="create a Conda environment")
    conda_create_parser.add_argument("-v", "--verbose", help="show the output of the Conda command", action="store_true")
    conda_create_parser.add_argument("--python", help="Python version to install", action="store", default="3.7.10")
    conda_create_parser.add_argument("--pip", help="Pip version to install", action="store", default="21.0.1")
    conda_create_parser.set_defaults(func=__create)

    # Add subparser for "get-activate-command" command
    conda_get_activate_parser = conda_subparsers.add_parser("get-activate-command", help="prints command to activate the Conda environment")
    conda_get_activate_parser.add_argument("-q", "--quiet", help="output the command without additional explanation for humans", action="store_true")
    conda_get_activate_parser.set_defaults(func=__get_activate_command)

    # Add subparser for "install" command
    conda_install_parser = conda_subparsers.add_parser("install", help="install Conda packages")
    conda_install_parser.add_argument("-v", "--verbose", help="show the output of the Conda command", action="store_true")
    conda_install_parser.add_argument("-c", "--channel", help="additional Conda channel to install from", action="append", default=[])
    conda_install_parser.add_argument("packages", metavar="PACKAGE", help="packages to install in Conda format ('$package_name=$package_version')", nargs='+')
    conda_install_parser.set_defaults(func=__install)

    # Add subparser for "pip-install" command
    conda_pip_install_parser = conda_subparsers.add_parser("pip-install", help="install PyPI packages")
    conda_pip_install_parser.add_argument("-v", "--verbose", help="show the output of the Conda command", action="store_true")
    conda_pip_install_parser.add_argument("packages", metavar="PACKAGE", help="packages to install in pip format ('$package_name==$package_version')", nargs='+')
    conda_pip_install_parser.set_defaults(func=__pip_install)

def __format_activate_str(conda_dir):
    return "conda activate %s" % pipes.quote(conda_dir)

def __create(args):
    conda_env = get_conda_env(args.preserve_id, args.framework_dir)
    if conda_env.exists():
        print("Conda environment at '%s' already exists." % conda_env.root)
        return

    conda_env.create(python_version=args.python, pip_version=args.pip, verbose=args.verbose)

    print("Conda environment created at '%s'." % conda_env.root)
    print("Activate the environment using '%s'." % __format_activate_str(conda_env.root))


def __get_activate_command(args):
    conda_env = get_conda_env(args.preserve_id, args.framework_dir)
    if not conda_env.exists():
        raise Error("Conda environment has not yet been created at '%s'." % conda_env.root)
    activate_str = __format_activate_str(conda_env.root)
    if args.quiet:
        print(activate_str)
    else:
        print("Activate the environment using '%s'." % activate_str)

def __install(args):
    conda_env = get_conda_env(args.preserve_id, args.framework_dir)
    conda_env.install(args.packages, args.channel, verbose=args.verbose)

def __pip_install(args):
    conda_env = get_conda_env(args.preserve_id, args.framework_dir)
    conda_env.pip_install(args.packages, verbose=args.verbose)
