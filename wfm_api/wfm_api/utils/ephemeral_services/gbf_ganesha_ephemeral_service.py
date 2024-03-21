"""
This class inherits from the EphemeralService class in order to provide ways to manipulate
and launch jobs with the Ganesha GBF plugin via Slurm.
"""
import os
from shlex import quote, split

from typing import Any, Dict, List, Tuple
from string import Template
import re
from loguru import logger

from wfm_api.config.wfm_settings import CommandSettings
from wfm_api.utils.misc_utils.misc_utils import run_cmd, run_cmd_output, remove_file
from wfm_api.utils.ephemeral_services.ephemeral_services import EphemeralService
from wfm_api.utils.misc_utils.misc_utils import check_isabspathname, check_issize
from wfm_api.utils.misc_utils.misc_utils import check_isabspathdir, get_newest_file, is_hestia_path
from wfm_api.utils.ephemeral_services.slurm_utils import get_bb_status
from wfm_api.utils.ephemeral_services.slurm_utils import generate_batch_file, is_lua_based
from wfm_api.utils.ephemeral_services.slurm_utils import build_slurm_ioi_options

__copyright__ = """
Copyright (C) Bull S. A. S.
"""


class GBFGaneshaEphemeralService(EphemeralService):
    """GBF ephemeral service class
    """
    def __init__(self, job_manager_commands: CommandSettings) -> None:
        """Initialize the GBF EphemeralService with appropriate values.

        Args:
            job_manager_commands(CommandSettings): job manager commands

        Returns:
            None
        """
        super().__init__()
        self.public_service_type = "NFS"
        self.service_type = "GBF"
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
        # A security hotspot has been raised here, but this is a false positive:
        # this string is actually a file name prefix.
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
        self.bbusecmd_template = Template(f"{self.job_submission_cmd} -J interactive "
                                          f"$partition_str -N 1 -n 1 --bb \"{self.job_submission_prefix} "
                                           "use_persistent Name=$service_name\" --pty bash")
        self.sbatch_string_output = "Submitted batch job "
        self.mandatory_keys = [ 'namespace', 'mountpoint', 'storagesize' ]
        self.optional_keys = [ 'location', 'datanodes' ]
        self.exported_vars = [ 'IOLIB_MODULES=EphemeralServices' ]

    def is_valid_hestia_source_file(self, fname: str) -> bool:
        """
        Given a file name, checks whether it has a valid hestia data-src format.

        Args:
            fname (str): the file to check
                         existence already checked by the caller

        Returns:
            bool: True if OK
        """
        try:
            # File should have 2 lines, each containing an objectid (header and data)
            with open(fname, 'r', encoding='utf-8') as fp:
                data = fp.read()
                lines = data.strip().split('\n')
                if len(lines) != 2:
                    logger.error(f"File {fname} has {len(lines)} lines, expected 2")
                    return False
                for line in lines:
                    if len(line.split()) != 1:
                        logger.error(f"File {fname} does not contain 1 objectid per line")
                        return False
                logger.debug(f"File {fname} is a valid hestia file")
                return True

        except IOError as err:
            logger.error(f"Error reading file {fname}: {err}")
            return False

    def is_valid_hestia_source_dir(self, directory: str) -> Tuple[bool, str]:
        """
        Given a directory name, checks whether its most recent file has a valid hestia format.

        Args:
            directory (str): the directory to check

        Returns:
            - (bool): True if OK
            - (str): if OK the newest file name (has a valid format)
        """
        # Get the newest file in the directory
        newest = get_newest_file(directory)
        if len(newest) == 0:
            logger.error(f"Directory {directory} is empty")
            return False, ''
        logger.debug(f"Newest file in {directory} : {newest}")
        return self.is_valid_hestia_source_file(newest), newest

    def process_dataset_file(self, destination: str) -> str:
        """
        Given a file name, generate the appropriate data_dst and data_src options.

        Args:
            destination (str): the dataset file name
                               Potentially starting with "HESTIA@"

        Returns:
            str: - the options string if successful
                 - empty string upon error
        """
        # 1st remove the potential hestia backend prefix
        # Note that we already checked that dstfile path is an absolute path
        hestia_backend, dstfile = is_hestia_path(destination)

        # If the destination is a directory
        #   - if the backend is hestia and it is a valid hestia directory
        #     - get the newest file if it has a valid hestia format
        #     - set it as both dst and src
        #   - else this is an error
        if os.path.isdir(dstfile):
            if hestia_backend:
                valid, newest_hestia_file = self.is_valid_hestia_source_dir(dstfile)
                if valid:
                    logger.debug(f"Directory {dstfile} is a valid hestia destination")
                    logger.debug(f"File {newest_hestia_file} has a valid hestia format")
                    bbdstsrc = f"data_dst={newest_hestia_file} data_src={newest_hestia_file}"
                else:
                    logger.error(f"Directory {dstfile} does not contain valid hestia files, "
                                  "cannot use it as a data source or destination")
                    bbdstsrc = ""
            else:
                logger.error(f"{dstfile} is not a file name, cannot use it "
                              "as a data source or destination")
                bbdstsrc = ""

            return bbdstsrc

        # If the destination does not exist, use the option data_dst=<that file>.
        # Note that we already checked during the WDF parsing that the owning directory was
        # specified as an absolute path, that it exists and can be accessed
        # (it is shared between the login node and datanodes and the WFM API is running on the
        # login node).
        # If the destination exists:
        #    empty file: error
        #    else:
        #       hestia backend:
        #          check that file is a valid hestia file
        #       use the options data_dst=<that file> data_src=<that file>
        if os.path.isfile(dstfile):
            if os.path.getsize(dstfile) > 0:
                logger.debug(f"file {dstfile} already exists, trying to use it "
                              "as a source and a destination")
                bbdstsrc = f"data_dst={destination} data_src={destination}"
                if hestia_backend and not self.is_valid_hestia_source_file(dstfile):
                    # Hestia backend, do more checks
                    # Error message logged by the called routine
                    bbdstsrc = ""
            else:
                logger.error(f"file {dstfile} is empty, cannot use it as a data source")
                bbdstsrc = ""
        else:
            logger.info(f"file {dstfile} does not exist, using it as a destination")
            bbdstsrc = f"data_dst={destination}"

        return bbdstsrc

    def start(self, srv: Dict[str, Any], workflow_name: str, run_id: str) -> int:
        """Starts an ephemeral service.

        Args:
            srv (Dict[str, Any]): the service description
            workflow_name (str): the workflow name to be used by IOI variables
            run_id (str): the session name suffixed by its starting timestamp

        Returns:
            int: The launched command return code. 0 on success
        """
        # The command to be run looks like:
        # srun --exclusive [-p<location>] --bb "GBF create_persistent Name=<name>
        #      StorageSize=<storagesize> Path=<mountpoint> FSType=ganesha StorageDataServers=1
        #      MetaDataServers=0 data_dst=<namespace> [data_src=<namespace>]"
        bbname = f"Name={srv['name']}"
        bbstoragesize = f"StorageSize={srv['attributes']['storagesize']}"
        bbpath = f"Path={srv['attributes']['mountpoint']}"
        # These 2 params are fixed for ganesha
        bbtype = "FSType=ganesha"
        bbmeta = "MetaDataServers=0"

        # We already checked that number of datanodes = 1 in check_service_attributes(), called
        # during services validation at the beginning of the session start.
        # So no need to check it once more here. We leave this sequence for the future when
        # more that 1 datanode will be supported.
        # Today we will have
        # - either no StorageDataServers specification (which means =1)
        # - or        StorageDataServers=1
        if 'datanodes' in srv['attributes'].keys():
            bbdatanodes = f"StorageDataServers={srv['attributes']['datanodes']}"
        else:
            bbdatanodes = ""

        dataset_file = srv['attributes']['namespace']
        bbdstsrc = self.process_dataset_file(dataset_file)
        if len(bbdstsrc) == 0:
            # Something went wrong - error message already output by the called routine
            return 1

        logger.info(f"extra options passed to create_persistent: {bbdstsrc}")

        bblaunch = (f"{self.job_submission_prefix} create_persistent {bbname} "
                    f"{bbstoragesize} {bbpath} {bbtype} {bbmeta} {bbdatanodes} {bbdstsrc}")

        extra_options = build_slurm_ioi_options(workflow_name, run_id, self.exported_vars)

        if 'location' in srv['attributes'].keys():
            partition_option = f"-p {srv['attributes']['location']}"
        else:
            partition_option = ""

        # We have to use srun in exclusive mode in that case.
        # Use quote() to avoid splitting the bb specification string that contains spaces
        launch_command = (f'{self.job_submission_cmd} --exclusive -J create {extra_options} '
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
                   or could not create data_dst file
        """
        logger.debug(f"SERVICE = {srv}")
        bbname = f"Name={srv['name']}"
        bbstoragesize = f"StorageSize={srv['attributes']['storagesize']}"
        bbpath = f"Path={srv['attributes']['mountpoint']}"
        # These 2 params are fixed for ganesha
        bbtype = "FSType=ganesha"
        bbmeta = "MetaDataServers=0"
        if 'datanodes' in srv['attributes'].keys():
            bbdatanodes = f"StorageDataServers={srv['attributes']['datanodes']}"
        else:
            bbdatanodes = ""

        dataset_file = srv['attributes']['namespace']
        bbdstsrc = self.process_dataset_file(dataset_file)
        if len(bbdstsrc) == 0:
            # Something went wrong - error message already output by the called routine
            return 0

        logger.info(f"extra options passed to create_persistent: {bbdstsrc}")

        bbspecs = f"{bbname} {bbstoragesize} {bbpath} {bbtype} {bbmeta} {bbdatanodes} {bbdstsrc}"
        if 'location' in srv['attributes'].keys():
            partition_option = f"-p {srv['attributes']['location']}"
        else:
            partition_option = ""

        # Generate the "create service" lines and write them to a temporary file
        specfile_name = self.generate_creation_batchfile(srv['name'], bbspecs)
        if not specfile_name:
            logger.error("Could not generate the create_persistent specfile")
            return 0

        extra_options = build_slurm_ioi_options(workflow_name, run_id, self.exported_vars)

        create_command = (f"{self.job_batch_cmd} --exclusive -J create {extra_options} "
                          f"{partition_option} {specfile_name}")
        logger.debug(f"create_command = {create_command}")
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
        remove_file( f"{self.bbuse_specfile_prefix}.{sname}")

        # Also remove the sbatch file related to this ephemeral service creation
        remove_file(f"{self.bbcreate_specfile_prefix}.{sname}")

        bbname = f"Name={sname}"
        bbstop = f"{self.job_submission_prefix} destroy_persistent {bbname}"

        if len(partition):
            partition_option = f"-p {partition}"
        else:
            partition_option = ""
        extra_options = build_slurm_ioi_options(workflow_name, run_id, self.exported_vars)

        # Generate the dependency option to make sure any service removal is started after the job
        # that started the ephemeral service completed.
        # A synchronously started ephemeral service is characterized by a sjobid param < 0,
        # no dependency is needed.
        if sjobid > 0:
            dependency_option = f"--dependency=afterany:{sjobid}"
        else:
            dependency_option = ""

        # Use quote() to avoid splitting the bb specification string that contains spaces
        stop_command = (f'{self.job_submission_cmd} --exclusive {partition_option} -J destroy '
                        f'{extra_options} {dependency_option} --bb {quote(bbstop)} hostname')
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

        extra_options = build_slurm_ioi_options(workflow_name, run_id, self.exported_vars)

        # Generate the dependency option to make sure any service removal is started after the job
        # that started the ephemeral service completed.
        # A synchronously started ephemeral service is characterized by a sjobid param < 0,
        # no dependency is needed.
        if sjobid > 0:
            dependency_option = f"--dependency=afterany:{sjobid}"
        else:
            dependency_option = ""

        destroy_command = (f"{self.job_batch_cmd} --exclusive -J destroy {extra_options} "
                           f"{partition_option} {dependency_option} {specfile_name}")
        ret_code, output_msg, error_msg = run_cmd_output(split(destroy_command))
        if ret_code != 0:
            if len(error_msg):
                logger.error(f"BB removal command reported an error: {error_msg}")
            return 0
        # Get the JobId
        jobid = output_msg.replace(self.sbatch_string_output, "")
        logger.debug(f"JobID = {jobid}")
        return jobid

    def remove_rm_temp_files(self, sname: str) -> None:
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
                   or command is not sbatch
        """
        # Generate the "use service" lines and write them to a temporary file
        specfile_name = self.generate_use_specfile(sname)
        if not specfile_name:
            logger.error("Could not generate the use_persistent specfile")
            return 0

        extra_options = build_slurm_ioi_options(workflow_name, run_id, self.exported_vars)

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
                             (f"{self.job_batch_cmd} --exclusive {extra_options} "
                              f"{dependency_option} --bbf {specfile_name}"),
                             command, count=1, flags=re.M)
        if use_command == command:
            # We did not find the batch command's absolute path in the step command,
            # try without path
            batch_cmd_short = os.path.basename(self.job_batch_cmd)
            use_command = re.sub(f"{batch_cmd_short}",
                                 (f"{batch_cmd_short} --exclusive {extra_options} "
                                  f"{dependency_option} --bbf {specfile_name}"),
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
        # Check mountpoint exists and is a directory and is writable
        # cannot be done here: we are not supposed to have the same access to
        # directories from the login and the compute nodes.
        # Only check the format (absolute pathname).
        directory = attributes['mountpoint']
        error_msg = check_isabspathname(directory)
        if len(error_msg) > 0:
            return f"mountpoint '{directory}' {error_msg}"

        # namespace is supposed to be a file name (we won't use the directory option
        # in this version).
        # It is the name of a file located on a directory shared by the datanodes and
        # login nodes.
        # Check namespace basedir exists and is a directory and is writable
        # 1st remove the potentialhestia backend prefix
        _, namespace = is_hestia_path(attributes['namespace'])
        directory = os.path.dirname(namespace)
        error_msg = check_isabspathdir(directory)
        if len(error_msg) > 0:
            return f"namespace directory '{directory}' {error_msg}"

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
        # We shouldn't have a mountpoint shared between different services
        mountpoints = [ service['attributes']['mountpoint'] for service in services ]
        logger.debug(f"MOUNTPOINTS to CHECK = {mountpoints}")
        # set() removes any duplicate
        if len(sorted(mountpoints)) != len(sorted(list(set(mountpoints)))):
            return (f"all the mountpoints should be distinct for the {self.public_service_type} "
                    "services")

        # We shouldn't have a namespace shared between different services
        namespaces = [ service['attributes']['namespace'] for service in services ]
        logger.debug(f"NAMESPACES to CHECK = {namespaces}")
        if len(sorted(namespaces)) != len(sorted(list(set(namespaces)))):
            return (f"all the namespaces should be distinct for the {self.public_service_type} "
                    "services")

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
        request['attributes']['gssize'] = srv['attributes']['storagesize']
        request['attributes']['mountpoint'] = srv['attributes']['mountpoint']
        return request
