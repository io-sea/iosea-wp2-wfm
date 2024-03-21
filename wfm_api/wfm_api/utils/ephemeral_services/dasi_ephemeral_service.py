"""
This class inherits from the GBFGaneshaEphemeralService class in order to provide ways to manipulate
and launch several GBFGaneshaEphemeralService for the DASI API.
"""
import os
from shlex import quote, split

from typing import Any, Dict, List
from fastapi import HTTPException
from loguru import logger

from wfm_api.config.wfm_settings import CommandSettings
from wfm_api.utils.ephemeral_services.gbf_ganesha_ephemeral_service import GBFGaneshaEphemeralService
from wfm_api.utils.misc_utils.misc_utils import check_isabspathname, check_issize
from wfm_api.utils.misc_utils.misc_utils import check_isabspathdir, is_hestia_path

__copyright__ = """
Copyright (C) Bull S. A. S.
"""


class DASIEphemeralService(GBFGaneshaEphemeralService):
    """DASI ephemeral service class
    """
    def __init__(self, job_manager_commands: CommandSettings) -> None:
        """Initialize the DASI EphemeralService with appropriate values.

        Args:
            job_manager_commands(CommandSettings): job manager commands

        Returns:
            None
        """
        super().__init__(job_manager_commands)
        self.public_service_type = "DASI"
        self.service_type = "DASI"

        self.mandatory_keys = [ 'dasiconfig', 'namespace', 'storagesize' ]
        self.optional_keys = [ 'location', 'datanodes' ]


    def start(self, srv: Dict[str, Any], workflow_name: str, run_id: str) -> int:
        """Starts an ephemeral service.

        Args:
            srv (Dict[str, Any]): the service description
            workflow_name (str): the workflow name to be used by IOI variables
            run_id (str): the session name suffixed by its starting timestamp

        Returns:
            int: The launched command return code. 0 on success
        """
        # TODO
        # to revisit for multi-roots
        detail_error = super().check_service_attributes(srv['attributes'])
        if detail_error:
            raise HTTPException(
                status_code = 404,
                detail = detail_error
            )
        return super().start(srv, workflow_name, run_id)
        
    def async_start(self, srv: Dict[str, Any], workflow_name: str, run_id: str) -> int:
        """Asynchronously starts an ephemeral service.

        Args:
            srv (Dict[str, Any]): the service description
            workflow_name (str): the workflow name to be used by IOI variables
            run_id (str): the session name suffixed by its starting timestamp

        Returns:
            int: JobId on success
                 0 if specfile could not be generated or launched command failed
                   or could not create data_dst file
        """
        # TODO
        # to revisit for multi-roots
        detail_error = super().check_service_attributes(srv['attributes'])
        if detail_error:
            raise HTTPException(
                status_code = 404,
                detail = detail_error
            )
        return super().async_start(srv, workflow_name, run_id)

    def stop(self, sname: str, sjobid: int, partition: str, workflow_name: str, run_id: str) -> int:
        """Stops an ephemeral service.

        Args:
            sname (str): the service name
            sjobid (int): the service starting job jobid
            partition (str): the partition where to run the stop command
            workflow_name (str): the workflow name to be used by IOI variables
            run_id (str): the session name suffixed by its starting timestamp

        Returns:
            int: The launched command return code. 0 on success
        """
        # TODO
        # to revisit for multi-roots
        return super().stop(sname, sjobid, partition, workflow_name, run_id)

    def async_stop(self,
                   sname: str,
                   sjobid: int,
                   partition: str,
                   workflow_name: str,
                   run_id: str) -> int:
        """Asynchronously stops an ephemeral service.

        Args:
            sname (str): the service name
            sjobid (int): the service starting job jobid
            partition (str): the partition where to run the stop command
            workflow_name (str): the workflow name to be used by IOI variables
            run_id (str): the session name suffixed by its starting timestamp

        Returns:
            int: JobId on success
                 0 if specfile could not be generated or launched command failed
        """
        # TODO
        # to revisit for multi-roots
        return super().async_stop(sname, sjobid, partition, workflow_name, run_id)

    def get_service_status(self, sname: str) -> str:
        """Gets the status of an ephemeral service.

        Args:
            sname (str): the service name

        Returns:
            str: The service status.
                 'UNKNOWN' if an error happened
        """
        # TODO
        # to revisit for multi-roots
        return super().get_service_status(sname)

    def use(self, sname: str, sjobid: int, command: str, workflow_name: str, run_id: str) -> int:
        """Uses an ephemeral service.

        Args:
            sname (str): the service name
            sjobid (int): the service starting job jobid
            command (str): the command to run on the service
            workflow_name (str): the workflow name to be used by IOI variables
            run_id (str): the session name suffixed by its starting timestamp

        Returns:
            int: JobId on success
                 0 if specfile could not be generated or launched command failed
                   or command is not sbatch
        """
        # TODO
        # to revisit for multi-roots
        return super().use(sname, sjobid, command, workflow_name, run_id)

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
        # TODO
        # to revisit for multi-roots
        return super().generate_use_cmd(sname,partition)

    def check_service_attributes(self, attributes: Dict[str, Any]) -> str:
        """Does specific checks if needed for the service attributes
        When entering this routine, we know that:
        - mandatory keys and values are present
        - each optional key has a corresponding value
        # TODO
        # to revisit for multi-roots
        - add and check mandatory keys for GBF ephemeral service (parent class)

        Args:
            attributes (Dict[str, Any]): the service attributes

        Returns:
            str: the detailed error message if there is one
                 empty string if no error
        """
        # Check dasi config file exists and is readable
        dasi_config = attributes['dasiconfig']
        error_msg = check_isabspathname(dasi_config)
        if len(error_msg) > 0:
            return f"The DASI configuration file '{dasi_config}' {error_msg}"

        try:
            config_file = open(dasi_config, "r", encoding="utf-8")
        except OSError:
            return f"Could not open DASI configuration file '{dasi_config}' for reading"
        config_file.close()

        # Check namespace exists and is a directory and is writable
        # (remove the potential hestia backend prefix)
        _, namespace = is_hestia_path(attributes['namespace'])
        error_msg = check_isabspathdir(namespace)
        if error_msg:
            return f"namespace directory '{namespace}' {error_msg}"

        # Check storagesize has a correct format
        size = attributes['storagesize']
        error_msg = check_issize(str(size))
        if len(error_msg) > 0:
            return f"storage size '{size}' {error_msg}"

        if 'datanodes' in list(attributes.keys()):
            if attributes['datanodes'] != 1:
                return f"number of datanodes can only be 1 for {self.public_service_type} services"

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
        logger.debug(f"SERVICES to CHECK = {services}")
        # We shouldn't have a dasiconfig file shared between different services
        dasiconfigs = [ service['attributes']['dasiconfig'] for service in services ]
        logger.debug(f"DASICONFIGS to CHECK = {dasiconfigs}")
        # set() removes any duplicate
        if len(sorted(dasiconfigs)) != len(sorted(list(set(dasiconfigs)))):
            return ("all the DASI config files should be distinct for the "
                    f"{self.public_service_type} services")

        # We shouldn't have a DASI roots shared between different DASI services
        # The check that namespaces are not shared between different services
        # is done before starting the service
        return ""

    def fill_reservation_request(self, srv: Dict[str, Any], user_name: str) -> Dict[str, Any]:
        """Fills in a reservation request related to the service in parameter
        All the mandatory keys in srv have already been checked

        Args:
            srv (Dict[str, Any]): the service as described in the WDF
            user_name (str): the calling user name

        Returns:
            Dict[str, Any]: the json request body
        """
        request = super().fill_reservation_request(srv, user_name)
        request['type'] = self.service_type
        return request
