#!/usr/bin/env python2

from __future__ import print_function
import os
import pipes
import subprocess

DEFAULT_FRAMEWORK_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "frameworks")

class InvalidSetupError(Exception): pass

def log(indentation, message):
    indent_str = ""
    while indentation > 1:
        indent_str += "|  "
        indentation -= 1
    if indentation == 1:
        indent_str += "|- "
    print(indent_str + message)

def create_log_fn(base_indentation, base_log=log):
    return lambda indentation, message: base_log(base_indentation + indentation, message)

def execute_command(command_line_list, verbose=False, shell=False):
    if verbose:
        execute_command_verbose(command_line_list, shell=shell)
    else:
        execute_command_quietly(command_line_list, shell=shell)

def execute_command_verbose(command_line_list, shell=False):
    """Executes a command, given as a list, while forwarding any output."""
    subprocess.check_call(command_line_list, shell=shell)

def execute_command_quietly(command_line_list, shell=False):
    """Executes a command, given as a list, while supressing any output."""
    with open(os.devnull, "wb") as devnull:
        subprocess.check_call(command_line_list, stdout=devnull, stderr=subprocess.STDOUT, shell=shell)

def execute_command_for_output(command_line_list):
    return subprocess.Popen(command_line_list, stdout=subprocess.PIPE).communicate()[0].decode("utf-8")

def write_remote_file(machine, filename, file_contents, file_permissions=None):
    ssh_command = ["ssh", machine, "mkdir -p %s; cat > %s" % (pipes.quote(os.path.dirname(filename)), pipes.quote(filename))]
    with open(os.devnull, "w") as devnull:
        cat_proc = subprocess.Popen(ssh_command, stdin=subprocess.PIPE, stdout=devnull, stderr=devnull)
        cat_proc.stdin.write(file_contents)
        cat_proc.stdin.close()
        cat_proc.wait()
    if file_permissions is not None:
        file_permissions_str = oct(file_permissions).zfill(4)
        execute_command(["ssh", machine, "chmod %s %s" % (file_permissions_str, pipes.quote(filename))])

