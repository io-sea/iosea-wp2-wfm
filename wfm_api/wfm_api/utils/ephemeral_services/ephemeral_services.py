"""
The goal of the EphemeralService class is to provide an abstraction of ephemeral
services. It provides the methods that must be implemented when adding support
for a new ephemeral service in the Workflow Manager.
"""
from shlex import split

from abc import abstractmethod
from typing import Any, Dict, List
from loguru import logger

from wfm_api.utils.database.wfm_database import ServiceStatus
from wfm_api.utils.misc_utils.misc_utils import run_cmd

__copyright__ = """
Copyright (C) Bull S. A. S.
"""
DEFAULT_COMMAND = '/bin/true'

class EphemeralService:
    """
    Represents an ephemeral service (defined as an abstract interface).

    This class provides methods to:
        - Get an ephemeral service status (get_service_status).
        - Start an ephemeral service (start).
        - Asynchronously start an ephemeral service (async_start).
        - Stop an ephemeral service (stop).
        - Asynchronously stop an ephemeral service (async_stop).
        - Remove any temporary file used to manage an ephemeral service (remove_rm_remp_files).
        - Use an ephemeral service (use).
        - Generate the command that should be used to interactively access an ephemeral service
          (generate_use_cmd).
        - Get the list of mandatory keys to be used in a WDF for this service attributes
          (get_mandatory_keys).
        - Get the list of optional keys to be used in a WDF for this service attributes
          (get_optional_keys).
        - Do specific checks for a given service attributes (check_service_attributes).
        - Do specific checks for a set of services (check_multi_service).
        - Fill in a reservation request based upon the resources needed (fill_reservation_request)
    """
    def __init__(self):
        """Initializes the instance variables
        """
        self.public_service_type = ''
        self.service_type = ''
        self.mandatory_keys = []
        self.optional_keys = []
        self.launch_command = DEFAULT_COMMAND
        self.stop_command = DEFAULT_COMMAND
        self.use_command = DEFAULT_COMMAND

    def start(self, srv: Dict[str, Any], workflow_name: str, run_id: str) -> int:
        """Starts an ephemeral service.

        Args:
            srv (Dict[str, Any]): the service description
            workflow_name (str): the workflow name to be used by IOI variables
            run_id (str): the session name suffixed by its starting timestamp

        Returns:
            int: The launched command return code. 0 on success
        """
        logger.info(f"Launching service {srv['name']} (type {srv['type']})"
                    f" for run {run_id} inside workflow {workflow_name}")
        return run_cmd(split(self.launch_command))

    @abstractmethod
    def async_start(self, srv: Dict[str, Any], workflow_name: str, run_id: str) -> int:
        """Asynchronously starts an ephemeral service.

        Args:
            srv (Dict[str, Any]): the service description
            workflow_name (str): the workflow name to be used by IOI variables
            run_id (str): the session name suffixed by its starting timestamp

        Returns:
            int: The resulting JobId. 0 on failure
        """

    def stop(self, sname: str, sjobid: int, partition: str, workflow_name: str, run_id: str) -> int:
        """Stops an ephemeral service

        Args:
            sname (str): the service name
            sjobid (int): the service starting job jobid
            partition (str): the partition where to run the command
            workflow_name (str): the workflow name to be used by IOI variables
            run_id (str): the session name suffixed by its starting timestamp

        Returns:
            int: The launched command return code. 0 on success
        """
        logger.info(f"Stopping service {sname} started by jobid {sjobid} in partition {partition}"
                    f" for run {run_id} inside workflow {workflow_name}")
        return run_cmd(split(self.stop_command))

    @abstractmethod
    def async_stop(self,
                   sname: str,
                   sjobid: int,
                   partition: str,
                   workflow_name: str,
                   run_id: str) -> int:
        """Asynchronously stops an ephemeral service

        Args:
            sname (str): the service name
            sjobid (int): the service starting job jobid
            partition (str): the partition where to run the command
            workflow_name (str): the workflow name to be used by IOI variables
            run_id (str): the session name suffixed by its starting timestamp

        Returns:
            int: The launched command return code. 0 on success
        """

    @abstractmethod
    def remove_rm_temp_files(self,
                             sname: str) -> None:
        """Removes any previously created temporary file for service creation or removal.

        Args:
            sname (str): the service name

        Returns:
            None
        """

    @abstractmethod
    def get_service_status(self, sname: str) -> ServiceStatus:
        """Gets an ephemeral service status.

        Args:
            sname (str): the service name

        Returns:
            ServiceStatus: The service status
        """

    def use(self, sname: str, sjobid: int, command: str, workflow_name: str, run_id: str) -> int:
        """Uses an ephemeral service

        Args:
            sname (str): the service name
            sjobid (int): the service starting job jobid
            command (str): the command to run using the service
            workflow_name (str): the workflow name to be used by IOI variables
            run_id (str): the session name suffixed by it starting timestamp

        Returns:
            int: The resulting JobId. 0 on failure
        """
        logger.info(f"Using service {sname} (started by {sjobid}) with command \"{command}\" "
                    f"- workflow name \"{workflow_name}\" - run_id \"{run_id}\"")
        return run_cmd(split(self.use_command))

    @abstractmethod
    def generate_use_cmd(self,
                         sname: str,
                         partition: str) -> str:
        """Generates the command that should be used to interactively
        access an ephemeral service.

        Args:
            sname (str): the service name
            partition (str): the partition where to run the command

        Returns:
            str: The command string
        """

    def get_mandatory_keys(self) -> List[str]:
        """Returns the mandatory keys to be used in a WDF for this service attributes

        Args:
            None

        Returns:
            List[str]: the list of keys that should be there
        """
        return self.mandatory_keys

    def get_optional_keys(self) -> List[str]:
        """Returns the optional keys to be used in a WDF for this service attributes

        Args:
            None

        Returns:
            List[str]: the list of keys that should be there
        """
        return self.optional_keys

    @abstractmethod
    def check_service_attributes(self, attributes: Dict[str, Any]) -> str:
        """Does specific checks if needed for the service attributes

        Args:
            attributes (Dict[str, Any]): the service attributes

        Returns:
            str: the detailed error message if there is one
                 empty string if no error
        """

    @abstractmethod
    def check_multi_service(self, services: List[Dict[str, Any]]) -> str:
        """Does specific checks if needed for a set of service

        Args:
            services (List[Dict[str, Any]]): the services to check

        Returns:
            str: the detailed error message if there is one
                 empty string if no error
        """

    def fill_reservation_request(self,
                                 srv: Dict[str, Any],
                                 user_name: str) -> Dict[str, Any]:
        """Fills the common part of a reservation request related to the service in parameter

        Args:
            srv (Dict[str, Any]): the service as described in the WDF
            user_name (str): the calling user name

        Returns:
            Dict[str, Any]: the json request body
                            should remain coherent with ServiceReservationItem declared in
                            rm_api/rm_api/models/resa_metadata.oy
        """
        request = {
            'name': srv['name'],
            'user': user_name,
            'user_slurm_token': 'MYTOKEN',
            'type': '',
            'servers': 1,
            'attributes': {},
            'location': []

        }
        if 'datanodes' in srv['attributes'].keys():
            request['servers'] = srv['attributes']['datanodes']
        if 'location' in srv['attributes'].keys():
            request['location'] = srv['attributes']['location'].split(',')
        return request
