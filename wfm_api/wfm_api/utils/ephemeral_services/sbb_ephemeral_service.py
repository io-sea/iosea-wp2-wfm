"""
This class inherits from the EphemeralService class in order to provide ways to manipulate
and launch jobs with the Smart Burst Buffer via Slurm.
"""
import os
from shlex import quote, split

from typing import Any, Dict, List
from string import Template
import re
from loguru import logger

from wfm_api.config.wfm_settings import CommandSettings
from wfm_api.utils.misc_utils.misc_utils import run_cmd, run_cmd_output, remove_file
from wfm_api.utils.ephemeral_services.ephemeral_services import EphemeralService
from wfm_api.utils.ephemeral_services.slurm_utils import get_bb_status
from wfm_api.utils.ephemeral_services.slurm_utils import generate_batch_file, is_lua_based
from wfm_api.utils.ephemeral_services.slurm_utils import build_slurm_ioi_options

__copyright__ = """
Copyright (C) Bull S. A. S.
"""


class SBBEphemeralService(EphemeralService):
    """SBB ephemeral service class
    """
    def __init__(self, job_manager_commands: CommandSettings) -> None:
        """Initialize the SBB EphemeralService with appropriate values.

        Args:
            job_manager_commands(CommandSettings): job manager commands

        Returns:
            None
        """
        super().__init__()
        self.public_service_type = "SBB"
        self.service_type = "SBB"
        if is_lua_based(job_manager_commands):
            self.is_lua_based = True
            self.job_submission_prefix = f"#BB_LUA {self.service_type}"
            self.job_batch_prefix = f"BB_LUA {self.service_type}"
        else:
            self.is_lua_based = False
            self.job_submission_prefix = f"{self.service_type}"
            self.job_batch_prefix = f"{self.service_type}"
        self.job_submission_cmd = job_manager_commands.job_submission_cmd
        self.job_control_cmd = job_manager_commands.job_control_cmd
        self.job_batch_cmd = job_manager_commands.job_batch_cmd
        # A security hotspot has been raised here, but this is a false positive:
        # this string is actually a file name prefix.
        self.bbcreate_specfile_prefix = "/tmp/bb.spec.create_persistent"
        self.bbcreate_template = Template("#!/bin/bash\n"
                                          "#SBATCH --output=/tmp/out-create_service-%j.txt\n"
                                          "#SBATCH --error=/tmp/err-create_service-%j.txt\n"
                                         f"#{self.job_batch_prefix} create_persistent $bbspecs\n"
                                          "srun hostname\n")
        self.bbdestroy_specfile_prefix = "/tmp/bb.spec.destroy_persistent"
        self.bbdestroy_template = Template("#!/bin/bash\n"
                                           "#SBATCH --output=/tmp/out-destroy_service-%j.txt\n"
                                           "#SBATCH --error=/tmp/err-destroy_service-%j.txt\n"
                                          f"#{self.job_batch_prefix} destroy_persistent $bbspecs\n"
                                           "srun hostname\n")
        # A security hotspot has been raised here, but this is a false positive:
        # this string is actually a file name prefix.
        self.bbuse_specfile_prefix = "/tmp/bb.spec.use_persistent"
        self.bbuse_template = Template(f"#!/bin/bash\n"
                                       f"#{self.job_batch_prefix} use_persistent Name=$service_name\n")
        self.sbatch_string_output = "Submitted batch job "
        self.bbusecmd_template = Template(f"{self.job_submission_cmd} -J interactive "
                                          f"$partition_str -N 1 -n 1 --bb \"{self.job_submission_prefix} "
                                          f"use_persistent Name=$service_name\" --pty bash")
        self.mandatory_keys = [ 'targets', 'flavor' ]
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
        bbname = f"Name={srv['name']}"
        if 'location' in srv['attributes'].keys():
            partition_option = f"-p {srv['attributes']['location']}"
        else:
            partition_option = ""
        bbflavor = f"Flavor={srv['attributes']['flavor']}"
        bbtargets = f"Targets={srv['attributes']['targets']}"
        if 'datanodes' in srv['attributes'].keys():
            bbdatanodes = f"Datanodes={srv['attributes']['datanodes']}"
        else:
            bbdatanodes = ""
        bblaunch = (f"{self.job_submission_prefix} create_persistent {bbname} "
                    f"{bbflavor} {bbtargets} {bbdatanodes}")

        extra_options = build_slurm_ioi_options(workflow_name, run_id, [])

        # Use quote() to avoid splitting the bb specification string that contains spaces
        launch_command = (f'{self.job_submission_cmd} -J create {extra_options} '
                          f'{partition_option} --bb {quote(bblaunch)} hostname')
        return run_cmd(split(launch_command))

    def generate_creation_batchfile(self, sname: str, bbspecs: str) -> str:
        """Generates a file that specifies a create_persistent request

        Args:
            sname (str): the service name
            bbspecs (str): the Burst Buffer specifications string

        Returns:
            str: The name of the generated spec file
                 Empty string if an error occured
        """
        return generate_batch_file(fname=f"{self.bbcreate_specfile_prefix}.{sname}",
                                   content=self.bbcreate_template.substitute(bbspecs=bbspecs))

    def async_start(self, srv: Dict[str, Any], workflow_name: str, run_id: str) -> int:
        """Asynchronously starts an ephemeral service.

        Args:
            srv (Dict[str, Any]): the service description
            workflow_name (str): the workflow name to be used by IOI variables
            run_id (str): the session name suffixed by its starting timestamp

        Returns:
            int: JobId on success
                 0 if specfile could not be generated or launched command failed
        """
        bbname = f"Name={srv['name']}"
        bbflavor = f"Flavor={srv['attributes']['flavor']}"
        bbtargets = f"Targets={srv['attributes']['targets']}"
        if 'datanodes' in srv['attributes'].keys():
            bbdatanodes = f"Datanodes={srv['attributes']['datanodes']}"
        else:
            bbdatanodes = ""
        bbspecs = f"{bbname} {bbflavor} {bbtargets} {bbdatanodes}"
        if 'location' in srv['attributes'].keys():
            partition_option = f"-p {srv['attributes']['location']}"
        else:
            partition_option = ""

        # Generate the "create service" lines and write them to a temporary file
        specfile_name = self.generate_creation_batchfile(srv['name'], bbspecs)
        if not specfile_name:
            logger.error("Could not generate the create_persistent specfile")
            return 0

        extra_options = build_slurm_ioi_options(workflow_name, run_id, [])

        create_command = (f"{self.job_batch_cmd} -J create {extra_options} {partition_option} "
                          f"{specfile_name}")
        ret_code, output_msg, error_msg = run_cmd_output(split(create_command))
        if ret_code != 0:
            remove_file(specfile_name)
            if len(error_msg):
                logger.error(f"BB creation command reported an error: {error_msg}")
            return 0
        # Get the JobId
        jobid = output_msg.replace(self.sbatch_string_output, "").strip()
        logger.info(f"CREATION JobID = {jobid}")
        return jobid

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
        # 1st remove the bbf specification file related to using this ephemeral service
        fname = f"{self.bbuse_specfile_prefix}.{sname}"
        if os.path.isfile(fname):
            logger.debug(f"Removing file {fname}")
            os.remove(fname)

        # Also remove the sbatch file related to this ephemeral service creation
        fname = f"{self.bbcreate_specfile_prefix}.{sname}"
        if os.path.isfile(fname):
            logger.debug(f"Removing file {fname}")
            os.remove(fname)

        bbname = f"Name={sname}"
        bbstop = f"{self.job_submission_prefix} destroy_persistent {bbname}"

        if len(partition):
            partition_option = f"-p {partition}"
        else:
            partition_option = ""
        extra_options = build_slurm_ioi_options(workflow_name, run_id, [])

        # Generate the dependency option to make sure any service removal is started after the job
        # that started the ephemeral service completed.
        # A synchronously started ephemeral service is characterized by a sjobid param < 0,
        # no dependency is needed.
        if sjobid > 0:
            dependency_option = f"--dependency=afterany:{sjobid}"
        else:
            dependency_option = ""

        # Use quote() to avoid splitting the bb specification string that contains spaces
        stop_command = (f'{self.job_submission_cmd} {partition_option} -J destroy {extra_options} '
                        f'{dependency_option} --bb {quote(bbstop)} hostname')
        return run_cmd(split(stop_command))

    def generate_destroy_batchfile(self, sname: str, bbspecs: str) -> str:
        """Generates a file that specifies a destroy_persistent request

        Args:
            sname (str): the service name
            bbspecs (str): the Burst Buffer specifications string

        Returns:
            str: The name of the generated spec file
                 Empty string if an error occured
        """
        return generate_batch_file(fname=f"{self.bbdestroy_specfile_prefix}.{sname}",
                                   content=self.bbdestroy_template.substitute(bbspecs=bbspecs))

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
        bbspecs = f"Name={sname}"

        # Generate the "destroy service" lines and write them to a temporary file
        specfile_name = self.generate_destroy_batchfile(sname, bbspecs)
        if not specfile_name:
            logger.error("Could not generate the destroy_persistent specfile")
            return 0

        if len(partition):
            partition_option = f"-p {partition}"
        else:
            partition_option = ""

        extra_options = build_slurm_ioi_options(workflow_name, run_id, [])

        # Generate the dependency option to make sure any service removal is started after the job
        # that started the ephemeral service completed.
        # A synchronously started ephemeral service is characterized by a sjobid param < 0,
        # no dependency is needed.
        if sjobid > 0:
            dependency_option = f"--dependency=afterany:{sjobid}"
        else:
            dependency_option = ""

        destroy_command = (f"{self.job_batch_cmd} -J destroy {extra_options} {partition_option} "
                           f"{dependency_option} {specfile_name}")
        ret_code, output_msg, error_msg = run_cmd_output(split(destroy_command))
        if ret_code != 0:
            remove_file(specfile_name)
            if len(error_msg):
                logger.error(f"BB removal command reported an error: {error_msg}")
            return 0
        # Get the JobId
        jobid = output_msg.replace(self.sbatch_string_output, "")
        logger.debug(f"JobID = {jobid}")
        return jobid

    def remove_rm_temp_files(self,
                             sname: str) -> None:
        """Removes any previously created temporary file for service creation, use or removal.

        Args:
            sname (str): the service name

        Returns:
            None
        """
        # 1st remove the sbatch file related to this ephemeral service creation
        remove_file(f"{self.bbcreate_specfile_prefix}.{sname}")

        # Then remove the bbf specification file related to using this ephemeral service
        remove_file(f"{self.bbuse_specfile_prefix}.{sname}")

        # Finally remove the sbatch file related to this ephemeral service removal
        remove_file(f"{self.bbdestroy_specfile_prefix}.{sname}")

    def get_service_status(self, sname: str) -> str:
        """Gets the status of an ephemeral service.

        Args:
            sname (str): the service name

        Returns:
            str: The service status.
                 'UNKNOWN' if an error happened
        """
        return get_bb_status(self.is_lua_based, self.job_control_cmd, sname)

    def generate_use_specfile(self, sname: str) -> str:
        """Generates a file that specifies a use_persistent request

        Args:
            sname (str): the service name

        Returns:
            str: The name of the generated spec file
                 Empty string if an error occured
        """
        return generate_batch_file(fname=f"{self.bbuse_specfile_prefix}.{sname}",
                                   content=self.bbuse_template.substitute(service_name=sname))

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
        """
        # Generate the "use service" lines and write them to a temporary file
        specfile_name = self.generate_use_specfile(sname)
        if not specfile_name:
            logger.error("Could not generate the use_persistent specfile")
            return 0

        extra_options = build_slurm_ioi_options(workflow_name, run_id, [])

        # Generate the dependency option to make sure any step is started after the job
        # that started the ephemeral service completed.
        # A synchronously started ephemeral service is characterized by a sjobid param < 0,
        # no dependency is needed.
        if sjobid > 0:
            dependency_option = f"--dependency=afterany:{sjobid}"
        else:
            dependency_option = ""

        # If the step command contains the batch command, substitue it with use specification
        use_command = re.sub(f"{self.job_batch_cmd}",
                             (f"{self.job_batch_cmd} {extra_options} {dependency_option} "
                              f"--bbf {specfile_name}"),
                             command, count=1, flags=re.M)
        if use_command == command:
            # We did not find the batch command's absolute path in the step command,
            # try without path
            batch_cmd_short = os.path.basename(self.job_batch_cmd)
            use_command = re.sub(f"{batch_cmd_short}",
                                 (f"{batch_cmd_short} {extra_options} {dependency_option} "
                                  f"--bbf {specfile_name}"),
                                 command, count=1, flags=re.M)
        if use_command == command:
            # Restrict the command to sbatch.
            logger.error(f"Step command (\"{command}\") does not contain the "
                         f"{batch_cmd_short} command")
            return 0

        ret_code, output_msg, error_msg = run_cmd_output(split(use_command))
        if ret_code != 0:
            if len(error_msg):
                logger.error(f"BB use command reported an error: {error_msg}")
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
            str: The command string
        """
        if len(partition):
            partition_opt = f"-p {partition}"
        else:
            partition_opt = ""

        return self.bbusecmd_template.substitute(partition_str=partition_opt, service_name=sname)

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
        All the mandatory keys in srv have already been checked

        Args:
            srv (Dict[str, Any]): the service as described in the WDF
            user_name (str): the calling user name

        Returns:
            Dict[str, Any]: the json request body
        """
        request = super().fill_reservation_request(srv, user_name)
        request['type'] = self.service_type
        request['attributes']['flavor'] = srv['attributes']['flavor']
        request['attributes']['targets'] = srv['attributes']['targets'].split(':')
        return request
