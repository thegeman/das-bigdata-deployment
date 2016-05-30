#!/usr/bin/env python2

from __future__ import print_function
import os

def log(indentation, message):
    indent_str = ""
    while indentation > 1:
        indent_str += "|  "
        indentation -= 1
    if indentation == 1:
        indent_str += "|- "
    print(indent_str + message)

def execute_command_quietly(command_line_list):
    """Executes a command, given as a list, while supressing any output."""
    with open(os.devnull, "wb") as devnull:
        subprocess.check_call(command_line_list, stdout=devnull, stderr=subprocess.STDOUT)
