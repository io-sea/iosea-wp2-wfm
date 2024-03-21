"""
Utility routines to be called when Slurm is used as a job scheduler.
"""
import os
import subprocess
from shlex import split
from typing import List
from loguru import logger

from wfm_api.config.wfm_settings import CommandSettings
from wfm_api.utils.database.wfm_database import ServiceStatus
from wfm_api.utils.misc_utils.misc_utils import run_cmd_output

__copyright__ = """
Copyright (C) Bull S. A. S.
"""


def get_status_from_bbuffer_line(line: str) -> str:
    """Parses a BB line from "scontrol show burst" or "scontrol show bbstat"
    to get the BB status out of it.

    Args:
        line (str): the line with the BB characteristics

    Returns:
        str: The BB status
    """
    # The line has one of the following formats:
    # 1. if output by scontrol show burst:
    # Name=lqcd-sbb1 CreateTime=2023-02-08T17:42:13 Pool=(null) Size=20MiB State=staged-in UserID=derbeyn(10579)
    # 2. if output by scontrol show bbstat:
    # FA: BB Type=GBF bbid=3589 Name=myPersistentBB State=staged-out CreateTime=2023-09-12T13:26:57
    sline = line.split()
    for item in sline:
        if 'State=' in item:
            return item.split('=')[1]
    return 'STOPPED'


def get_bbstatus_from_scontrol_show_burst_output(bbname: str, output: str) -> str:
    """Parses an output from "scontrol show burst" to get a BB status out of it.

    Args:
        bbname (str): the service name we are looking for in the command output.
        output (str): the command output.

    Returns:
        str: The info we are looking for.
             'STOPPED' if the burst buffer was not found in the scontrol command output
    """
    # Parses the output from 'scontrol show burst'.
    # This output looks as follows when there are burst buffers allocated:
    #
    # Name=bull_sbb DefaultPool=(null) Granularity=1 TotalSpace=180MiB FreeSpace=160MiB UsedSpace=20MiB
    #   Flags=EnablePersistent
    #   StageInTimeout=86400 StageOutTimeout=86400 ValidateTimeout=5 OtherTimeout=5
    #   GetSysState=/usr/libexec/flash-accelerators/slurm/sbb.sh
    #   GetSysStatus=(null)
    #   Allocated Buffers:
    #     Name=lqcd-sbb1 CreateTime=2023-02-08T17:42:13 Pool=(null) Size=20MiB State=staged-in UserID=derbeyn(10579)
    #   Per User Buffer Use:
    #     UserID=derbeyn(10579) Used=20MiB
    #
    # It looks as follows when there are no burst buffers allocated
    # (the 'Allocated Buffers' part is not present):
    #
    # Name=bull_sbb DefaultPool=(null) Granularity=1 TotalSpace=180MiB FreeSpace=160MiB UsedSpace=20MiB
    #   Flags=EnablePersistent
    #   StageInTimeout=86400 StageOutTimeout=86400 ValidateTimeout=5 OtherTimeout=5
    #   GetSysState=/usr/libexec/flash-accelerators/slurm/sbb.sh
    #   GetSysStatus=(null)

    # Look for the string 'Name=burstbuffername ' in the output lines
    bbname = f"Name={bbname} "
    buffers = 0
    for line in output.split('\n'):
        if 'Allocated Buffers' in line:
            buffers = 1
        if buffers and (bbname in line):
            return get_status_from_bbuffer_line(line)
    # We didn't find the service in the scontrol command output. This probably
    # means that it was staged-out and that its info is not in the slurm
    # statistics anymore.
    return 'STOPPED'


def get_bbstatus_from_scontrol_show_bbstat_output(bbname: str, output: str) -> str:
    """Parses an output from "scontrol show bbstat" to get a BB status out of it.

    Args:
        bbname (str): the service name we are looking for in the command output.
        output (str): the command output.

    Returns:
        str: The info we are looking for.
             'STOPPED' if the burst buffer was not found in the scontrol command output
    """
    # Parses the output from 'scontrol show bbstat'.
    # This output looks as follows when there are burst buffers allocated:
    #
    # FA: BB Type=GBF bbid=3589 Name=myPersistentBB State=staged-out CreateTime=2023-09-12T13:26:57
    # FA: Total storage : 450GiB
    # FA: Used  storage : 0
    # FA: Free  storage : 450GiB
    # FA: Total memory  : 130GiB
    # FA: Used  memory  : 0
    # FA: Free  memory  : 130GiB

    # Look for the string ' Name=burstbuffername ' in the output lines
    bbname = f" Name={bbname} "
    for line in output.split('\n'):
        if line.startswith('FA: BB ') and (bbname in line):
            return get_status_from_bbuffer_line(line)
    # We didn't find the service in the scontrol command output. This probably
    # means that it was staged-out and that its info is not in the slurm
    # statistics anymore.
    return 'STOPPED'


def generate_batch_file(fname: str, content: str) -> str:
    """Generates a file if not already existing.
    This file is a BB specification (use or create) that will be used in sbatch mode.

    Args:
        fname (str): the file name
        content (str): the file contents

    Returns:
        str: The name of the generated file
             Empty string if an error occured
    """
    if os.path.isfile(fname):
        logger.info(f"{fname} already exists, will be overwritten")
    else:
        logger.debug(f"{fname} does not exist, will be created")

    try:
        specfile = open(fname, 'w', encoding="utf-8")
    except OSError:
        logger.error(f"Could not create file {fname}")
        return ""

    with specfile:
        specfile.write(content)
        logger.debug("BEGIN {fname} contents: ============")
        logger.debug(content)
        logger.debug("END {fname} contents: ============")

    return fname


def build_slurm_ioi_options(workflow_name: str, run_id: str, extra_exports: List[str]) -> str:
    """Build the extra options that will be provided to slurm to get IOI instrumentation.

    Args:
        workflow_name (str): the workflow name to be used by IOI variables
        run_id (str): the session name suffixed by it starting timestamp
        extra_exports (List[str]): list of other env vars settings
                                   that should be exported too

    Returns:
        str: the extra options
    """
    other_exports = ""
    if len(extra_exports):
        for exp in extra_exports:
            other_exports += f",{exp}"

    return (f"--export=ALL{other_exports},IOI_WORKFLOW_NAME={workflow_name},IOI_WORKFLOW_RUN_ID={run_id} "
             "--ioinstrumentation=yes")


def is_lua_based(job_manager_commands: CommandSettings) -> bool:
    """Checks whether current slurm implementation uses Lua scripts or C plugins

    Args:
        job_manager_commands (CommandSettings): job manager commands

    Returns:
        bool: True if slurm uses Lua scripts
              False else
    """
    cmd = f"{job_manager_commands.job_control_cmd} show config"
    cmdret = subprocess.run(split(cmd), capture_output=True, check=False)
    output =  cmdret.stdout.decode("utf-8")
    for line in output.split('\n'):
        if 'BurstBufferType' in line:
            fields = line.split('=')
            return fields[1].strip() == 'burst_buffer/lua'
    return False


def get_bb_status(lua_based: bool, job_control_cmd: str, sname: str) -> str:
    """Gets the status of an ephemeral service given its name.

    Args:
        lua_based (bool): whether the running slurm is based on Lua scripts
        job_control_cmd (str): the command to use to get job control
        sname (str): the service name

    Returns:
        str: The service status.
             'UNKNOWN' if an error happened
    """
    if lua_based:
        status_command = f"{job_control_cmd} show bbstat"
        get_status_from_output = 'get_bbstatus_from_scontrol_show_bbstat_output'
    else:
        status_command = f"{job_control_cmd} show burst"
        get_status_from_output = 'get_bbstatus_from_scontrol_show_burst_output'

    ret_code, output_msg, error_msg = run_cmd_output(split(status_command))
    if ret_code != 0:
        if len(error_msg):
            logger.error(f"BB status command reported an error: {error_msg}")
        status_str = ServiceStatus.UNKNOWN.value
    else:
        status = eval(get_status_from_output)(sname, output_msg)
        if status == 'staged-in':
            status_str = ServiceStatus.ALLOCATED.value
        elif status == 'staged-out':
            status_str = ServiceStatus.STOPPED.value
        elif status == 'staging-out':
            status_str = ServiceStatus.STOPPING.value
        else:
            status_str = ServiceStatus[status.replace("-", "").upper()].value

    return status_str
