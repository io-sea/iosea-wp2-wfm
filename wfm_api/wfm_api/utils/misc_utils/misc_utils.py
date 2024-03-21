"""
This module defines miscellaneous utility routines used
by the services and the job manager routines.
"""
import os
import subprocess
import re

from typing import List, Tuple
from loguru import logger


__copyright__ = """
Copyright (C) Bull S. A. S.
"""


def run_cmd(cmd: List[str]) -> int:
    """Runs a command and returns its returns code.

    Args:
        cmd (List[str]): the command to run, split into a list of strings

    Returns:
        int: the command return code
    """
    logger.info(f"Running command: {cmd}")
    # We intentionally leave check=false to avoid raising an except upon
    # command exiting with 1
    cmdret = subprocess.run(cmd, check=False)
    if cmdret.returncode != 0:
        logger.warning(f"Command output non-zero return code: code {cmdret.returncode}")
    return cmdret.returncode


def run_cmd_output(cmd: List[str]) -> Tuple[int, str, str]:
    """Runs a command and returns its return code, its stdout and its stderr.

    Args:
        cmd (List[str]): the command to run, split into a list of strings

    Returns:
        Tuple[int, str, str]: the command rc, stdout and stderr
    """
    logger.info(f"Running command: {cmd}")
    # We intentionally leave check=false to avoid raising an except upon
    # command exiting with 1
    cmdret = subprocess.run(cmd, capture_output=True, check=False)
    if cmdret.returncode != 0:
        logger.warning(f"Command output non-zero return code: code {cmdret.returncode}")
    return cmdret.returncode, cmdret.stdout.decode("utf-8"), cmdret.stderr.decode("utf-8")


def remove_file(fname: str) -> None:
    """Removes a named file.

    Args:
        fname (str): the file to remove

    Returns:
        None
    """
    if os.path.isfile(fname):
        logger.debug(f"Removing file {fname}")
        os.remove(fname)

def check_isabspathname(directory: str) -> str:
    """Given a name, check it is an absolute path name

    Args:
        directory: the path to check

    Returns:
        str: the detailed error message if there is one
             empty string if no error
    """
    if len(directory) <= 1:
        return "is not a correct directory name"
    if directory[0] != '/':
        return "is not an absolute pathname"
    return ""


def check_isabspathdir(directory: str) -> str:
    """Given a name, check it is an absolute path that corresponds
    to a readable, writable directory,

    Args:
        directory: the path to check

    Returns:
        str: the detailed error message if there is one
             empty string if no error
    """
    error_msg = check_isabspathname(directory)
    if len(error_msg) > 0:
        return error_msg

    if not os.path.isdir(directory):
        return "is not a directory or does not exist"
    if not os.access(directory, os.R_OK | os.W_OK | os.X_OK):
        return "cannot be accessed"
    return ""


def check_issize(size: str) -> str:
    """Given a string, check it has a correct size format:
    <int> / <int>K / <int>Ki
    <int> / <int>M / <int>Mi
    <int> / <int>G / <int>Gi
    ...

    Args:
        size (str): the string to check

    Returns:
        str: the detailed error message if there is one
             empty string if no error
    """
    if ' ' in size:
        return "is not a correct size format"

    # Remove potential B from the end of the string
    retcode, stdout, stderr = run_cmd_output(['numfmt', '--from=auto', size.split("B")[0]])
    if retcode == 0:
        return ""
    return "is not a correct size format"


def get_newest_file(path: str) -> str:
    """Returns the newest file present in a directory.

    Args:
        path (str): the directory name
                    existence and type already checked by the caller

    Returns:
        str: name of the newest file
             empty string if no files in there
    """
    try:
        files = os.listdir(path)

    except IOError as err:
        logger.error(f"Error reading directory {path}: {err}")
        files = []

    if len(files) == 0:
        return ""

    paths = [os.path.join(path, basename) for basename in files]
    return max(paths, key=os.path.getctime)


def is_hestia_path(input_string: str) -> Tuple[bool, str]:
    """Checks whether the input string has the format "HESTIA@XXX"

    Args:
        input_string (str): the string to check

    Returns:
        Tuple[bool, str]:
             - True: if OK
             - path w/o the potential "HESTIA@" prefix
    """
    path = re.sub(r'^{0}'.format(re.escape('HESTIA@')), '', input_string)
    return path != input_string, path
