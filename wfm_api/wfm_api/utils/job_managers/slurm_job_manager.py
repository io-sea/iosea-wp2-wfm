"""
This class inherits from the JobManager class in order to provide ways to manipulate
and launch jobs with Slurm.
"""
from typing import Any, Dict, List
from shlex import split

from string import Template
from loguru import logger

from wfm_api.config.wfm_settings import CommandSettings
from wfm_api.utils.job_managers.job_managers import JobManager
from wfm_api.utils.job_managers.slurm_utils import SlurmJobStatus, WFMJobStatus
from wfm_api.utils.misc_utils.misc_utils import run_cmd, run_cmd_output

__copyright__ = """
Copyright (C) Bull S. A. S.
"""


class SlurmJobManager(JobManager):
    """SLURM JobManager class
    """
    def __init__(self, job_manager_commands: CommandSettings) -> None:
        """Initialize the SLURM JobManager with appropriate values.

        Args:
            job_manager_commands(CommandSettings): job manager commands

        Returns:
            None
        """
        super().__init__()
        self.job_mgr_name = "SLURM"
        self.job_state_cmd = job_manager_commands.job_state_cmd
        self.job_state_template = Template(f"{self.job_state_cmd} -h --job $jobid --format=\"%T\"")
        self.job_cancel_cmd = job_manager_commands.job_cancel_cmd
        self.job_control_cmd = job_manager_commands.job_control_cmd
        self.failure_status = [ 'BOOT_FAIL', 'DEADLINE', 'FAILED', 'NODE_FAIL', 'OUT_OF_MEMORY',
                                'TIMEOUT' ]
        self.held_or_requeued_status = [ 'RESV_DEL_HOLD', 'REQUEUE_FED', 'REQUEUE_HOLD' ]
        self.waiting_status = [ 'CONFIGURING', 'PENDING' ]
        self.special_status = [ 'RESIZING', 'SIGNALING' ]
        self.running_status = [ 'RUNNING' ]
        self.stopping_status = [ 'COMPLETING', 'STAGE_OUT', 'REQUEUED' ]
        self.stopped_status = [ 'CANCELLED', 'COMPLETED', 'PREEMPTED', 'REVOKED', 'SPECIAL_EXIT',
                                'STOPPED', 'SUSPENDED' ]
        self.unstoppable_status = [ 'CONFIGURING', 'COMPLETING', 'PENDING', 'RUNNING', 'RESV_DEL_HOLD',
                                    'REQUEUE_FED', 'REQUEUE_HOLD', 'REQUEUED', 'RESIZING', 'SIGNALING',
                                    'STAGE_OUT', 'SUSPENDED' ]


    def to_rm_job_status(self, status: str) -> str:
        """Returns the Slurm view of a WFM job status.

        Args:
            status (str): the job status

        Returns:
            str: the slurm job status
        """
        # Special status values that exist internally but are not supported by slurm
        if status == 'STARTING':
            return status
        return SlurmJobStatus[status].value

    def to_wfm_job_status(self, status: str) -> str:
        """Returns the WFM view of a slurm job status.

        Args:
            status (str): the job status

        Returns:
            str: the internally managed job status
        """
        return WFMJobStatus[status].value

    def get_job_status(self, jobid: int) -> str:
        """Gets a job status.

        Args:
            jobid (int): the job id

        Returns:
            str: The job status as returned by squeue.
                 If heterogenous job: blank separated status strings
        """
        status_command = self.job_state_template.substitute(jobid=jobid)
        ret_code, status, error_msg = run_cmd_output(split(status_command))
        if ret_code != 0:
            # This means that the job was not found via squeue.
            # So that it finished executing a long time ago.
            return SlurmJobStatus.STOPPED.value

        # Check for empty status
        # This means that the job just finished executing.
        if not status:
            return SlurmJobStatus.STOPPED.value

        # Note that the returned status might be a concatenation of different status strings
        # in the case of a heterogenous job.
        # Format = "<status_component_0>\n<status_component_1>\n    \n<status_component_X>\n"
        # In that case, we need to convert each component status one by one.
        # We leave them all in a single string that is stored in the DB.
        # This string is then processed differently depending on what the status is needed for.
        step_status = ''
        for stat in status.split():
            slurm_component_status = SlurmJobStatus[stat.upper()].value
            step_status = " ".join([step_status.lstrip(), slurm_component_status]).lstrip()

        logger.info(f"jobid = {jobid} - status = {step_status}")

        return step_status

    def cancel_job(self, jobid: int) -> int:
        """Cancels a job

        Args:
            jobid (int): the job id to cancel

        Returns:
            int: The launched command return code. 0 on success
        """
        logger.info(f"Canceling job \"{jobid}\"")
        cancel_command = f'{self.job_cancel_cmd} {jobid}'
        return run_cmd(split(cancel_command))

    def get_usable_locations(self) -> List[Dict[str, Any]]:
        """Gets all partition names

        Args:
            None

        Returns:
            List[Dict[str, Any]]: The partitions names
        """
        logger.info("Getting all partitions")
        get_partitions_command = f"{self.job_control_cmd} --hide -o show partitions"
        ret_code, output, error_msg = run_cmd_output(split(get_partitions_command))
        if ret_code != 0:
            if len(error_msg):
                logger.error(f"Command \"{get_partitions_command}\" failed: {error_msg}")
            else:
                logger.error(f"Command \"{get_partitions_command}\" failed")
            return []

        # The output of scontrol --hide -o show partitions looks like:
        # PartitionName=part0 <many other infos related to part0>
        # ...
        # PartitionName=partN <many other infos related to partN>
        #
        # Only take the names
        if len(output) == 0:
            logger.error("No partition available")
            return []

        output = output.rstrip()
        partitions = []
        for line in output.split('\n'):
            pname = line.split()[0].split('=')[1]
            partitions += [ {'name': pname } ]

        logger.info(f"Got partitions : {partitions}")
        return partitions

    def combine_step_status_for_output(self, status: str) -> str:
        """Combine a set of blank-separated status strings into a single one,
        aimed for output.
        This is necessary to support heterogenous jobs.
        The rules applied to do the combination are described here:
        https://confluencebdsfr.fsc.atos-services.net/display/BRDM/supporting+heterogenous+jobs+in+the+WFM

        Args:
            status: the step status (potentially several blank-separated status strings)

        Returns:
            str: The combined status
        """
        # 1st convert the string into a list of strings
        status_list = status.split()
        if len(status_list) == 1:
            # single status, just return it
            return status

        for cur_stat in status_list:
            if cur_stat in self.failure_status:
                return cur_stat

        # No job component in failure

        for cur_stat in status_list:
            if cur_stat in self.held_or_requeued_status:
                return cur_stat

        # No job component in failure nor held or requeued

        for cur_stat in status_list:
            if cur_stat in self.waiting_status:
                return cur_stat

        # No job component
        # - in failure
        # - nor held or requeued
        # - nor waiting

        for cur_stat in status_list:
            if cur_stat in self.special_status:
                return cur_stat

        # No job component
        # - in failure
        # - nor held or requeued
        # - nor waiting
        # - nor signaling or resizing

        for cur_stat in status_list:
            if cur_stat in self.running_status:
                return cur_stat

        # No job component
        # - in failure
        # - nor held or requeued
        # - nor waiting
        # - nor signaling or resizing
        # - nor running

        for cur_stat in status_list:
            if cur_stat in self.stopping_status:
                return cur_stat

        # The only remaining cases are stopped states
        return SlurmJobStatus.STOPPED.value

    def combine_step_status_for_stopping(self, status: str) -> str:
        """Combine a set of blank-separated status strings into a single one,
        aimed for stopping the associated job.
        This is necessary to support heterogenous jobs.
        The rules applied to do the combination are described here:
        https://confluencebdsfr.fsc.atos-services.net/display/BRDM/supporting+heterogenous+jobs+in+the+WFM

        Args:
            status: the step status (potentially several blank-separated status strings)

        Returns:
            str: The combined status
        """
        # 1st convert the string into a list of strings
        status_list = status.split()
        if len(status_list) == 1:
            # single status, just return it
            return status

        for cur_stat in status_list:
            if cur_stat in self.unstoppable_status:
                return cur_stat

        # The only remaining cases are stoppable states
        return SlurmJobStatus.STOPPED.value
