"""
This class inherits from the EphemeralService class in order to provide ways to manipulate
and launch jobs without any accelerator via Slurm.
"""
import os
from shlex import split

from typing import Any, Dict, List
import re
from loguru import logger

from wfm_api.config.wfm_settings import CommandSettings
from wfm_api.utils.database.wfm_database import ServiceStatus
from wfm_api.utils.misc_utils.misc_utils import run_cmd_output
from wfm_api.utils.ephemeral_services.ephemeral_services import EphemeralService
from wfm_api.utils.ephemeral_services.slurm_utils import build_slurm_ioi_options

__copyright__ = """
Copyright (C) Bull S. A. S.
"""


class NOEphemeralService(EphemeralService):
    """NO ephemeral service class
    """
    def __init__(self, job_manager_commands: CommandSettings) -> None:
        """Initialize the NO EphemeralService with appropriate values.

        Args:
            job_manager_commands(CommandSettings): job manager commands

        Returns:
            None
        """
        super().__init__()
        self.public_service_type = 'NONE'
        self.service_type = "NONE"
        self.job_batch_cmd = job_manager_commands.job_batch_cmd
        self.sbatch_string_output = "Submitted batch job "

    def start(self, srv: Dict[str, Any], workflow_name: str, run_id: str) -> int:
        """Does nothing, since there is no ephemeral service.

        Args:
            srv (Dict[str, Any]): the service description
            workflow_name (str): the workflow name to be used by IOI variables
            run_id (str): the session name suffixed by its starting timestamp

        Returns:
            0 always
        """
        logger.error("No service to launch synchronously")
        return 0

    def async_start(self, srv: Dict[str, Any], workflow_name: str, run_id: str) -> int:
        """Does nothing, since no ephemeral service.

        Args:
            srv (Dict[str, Any]): the service description
            workflow_name (str): the workflow name to be used by IOI variables
            run_id (str): the session name suffixed by its starting timestamp

        Returns:
            -1 always - this is to denote that there is no service underneath
        """
        logger.error("No service to launch asynchronously")
        # We don't mind about the returned code: this routine is supposed to never be called
        # if no ephemeral service.
        return -1

    def stop(self, sname: str, sjobid: int, partition: str, workflow_name: str, run_id: str) -> int:
        """Does nothing, since no ephemeral service.

        Args:
            sname (str): the service name
            sjobid (int): the service starting job jobid
            partition (str): the partition where to run the stop command
            workflow_name (str): the workflow name to be used by IOI variables
            run_id (str): the session name suffixed by its starting timestamp

        Returns:
            0 always
        """
        logger.error("No service to stop synchronously")
        return 0

    def async_stop(self,
                   sname: str,
                   sjobid: int,
                   partition: str,
                   workflow_name: str,
                   run_id: str) -> int:
        """Does nothing, since no ephemeral service.

        Args:
            sname (str): the service name
            sjobid (int): the service starting job jobid
            partition (str): the partition where to run the stop command
            workflow_name (str): the workflow name to be used by IOI variables
            run_id (str): the session name suffixed by its starting timestamp

        Returns:
            -1 always - this is to denote that there is no service underneath
        """
        logger.error("No service to stop asynchronously")
        # We don't mind about the returned code: this routine is supposed to never be called
        # if no ephemeral service
        return -1

    def remove_rm_temp_files(self, sname: str) -> None:
        """Removes any previously created temporary file for service creation, use or removal.

        Args:
            sname (str): the service name

        Returns:
            None
        """
        return

    def get_service_status(self, sname: str) -> str:
        """Gets the status of an ephemeral service.

        Args:
            sname (str): the service name

        Returns:
            str: 'UNKNOWN' always
        """
        logger.error("Returning UNKNOWN status")
        return ServiceStatus.UNKNOWN.value

    def use(self, sname: str, sjobid: int, command: str, workflow_name: str, run_id: str) -> int:
        """Runs a command without an ephemeral service.

        Args:
            sname (str): the service name
            sjobid (int): the service starting job jobid
            command (str): the command to run on the service
            workflow_name (str): the workflow name to be used by IOI variables
            run_id (str): the session name suffixed by its starting timestamp

        Returns:
            int: JobId on success
                 0 if launched command failed or command is not sbatch
        """
        if len(sname) > 0:
            logger.error(f"Expected empty service name - Got {sname}")

        extra_options = build_slurm_ioi_options(workflow_name, run_id, [])

        # If the step command contains the batch command, extend it with the extra options
        use_command = re.sub(f"{self.job_batch_cmd}", f"{self.job_batch_cmd} {extra_options}",
                             command, count=1, flags=re.M)
        if use_command == command:
            # We did not find the batch command's absolute path in the step command,
            # try without path
            batch_cmd_short = os.path.basename(self.job_batch_cmd)
            use_command = re.sub(f"{batch_cmd_short}", f"{batch_cmd_short} {extra_options}",
                                 command, count=1, flags=re.M)
        if use_command == command:
            # Restrict the command to sbatch.
            logger.error(f"Step command (\"{command}\") does not contain the "
                         f"{batch_cmd_short} command")
            return 0

        ret_code, output_msg, error_msg = run_cmd_output(split(use_command))
        if ret_code != 0:
            if len(error_msg):
                logger.error(f"Step command reported an error: {error_msg}")
            return 0
        # Get the JobId
        jobid = output_msg.replace(self.sbatch_string_output, "").strip()
        logger.debug(f"JobID = {jobid}")
        return jobid

    def generate_use_cmd(self,
                         sname: str,
                         partition: str) -> str:
        """Generates the command that should be used to interactively
        access an ephemeral service.

        Args:
            sname (str): the service name
            partition (str): the partition where to run the command

        Returns:
            str: always empty
        """
        return ""

    def check_service_attributes(self, attributes: Dict[str, Any]) -> str:
        """Does specific checks if needed for the service attributes
        When entering this routine, we know that:
        - mandatory keys and values are present
        - each optional key has a corresponding value

        Args:
            attributes (Dict[str, Any]): the service attributes

        Returns:
            str: the detailed error message if there is one
                 empty string if no error
        """
        return ""

    def check_multi_service(self, services: List[Dict[str, Any]]) -> str:
        """Does specific checks if needed for a set of services
        When entering this routine, we know that:
        - mandatory keys and values are present
        - each optional key has a corresponding value

        Args:
            services (List[Dict[str, Any]]): the services to check

        Returns:
            str: the detailed error message if there is one
                 empty string if no error
        """
        return ""

    def fill_reservation_request(self, srv: Dict[str, Any], user_name: str) -> Dict[str, Any]:
        """Fills in a reservation request related to the service in parameter

        Args:
            srv (Dict[str, Any]): the service to fill a reservation request for
            user_name (str): the calling user name

        Returns:
            Dict[str, Any]: the json request body
        """
        return {}
