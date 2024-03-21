"""This module defines miscellaneous utility routines used by the WFM API.
"""
import os
import time
import re
import hashlib
from typing import Any, Dict, List, Tuple
from datetime import datetime
from fastapi import HTTPException
from loguru import logger


from wfm_api.config.wfm_settings import CommandSettings, ResourcemanagerSettings
from wfm_api.config.dasi_settings import DASISettings
from wfm_api.utils.database.wfm_database import WFMDatabase, ServiceStatus, StepStatus
from wfm_api.utils.database.wfm_database import SessionStatus
from wfm_api.utils.misc_utils.misc_utils import check_isabspathname
from wfm_api.utils.errors import UnexistingSessionNameError
from wfm_api.utils import EPHEMERAL_SERVICES, JOB_MANAGERS, RESOURCE_MANAGERS

__copyright__ = """
Copyright (C) Bull S.A.S.
"""

# TODO:
# 1. Move the keys to a model definition
# 2. make all the WDF analysis below done by pydantic.
# model in iopa repo:
# iopa/IOanalytics/ioanalyticstools/ioanalyticstools/configuration_parser.py
# +
# iopa/IOanalytics/optim_lab/optim_lab/config_parser.py

wdf_mandatory_keys = [ 'workflow', 'services', 'steps' ]
wdf_optional_keys = []
workflow_mandatory_keys = [ 'name' ]
workflow_optional_keys = []
services_mandatory_keys = [ 'name', 'type', 'attributes' ]
services_optional_keys = []
steps_mandatory_keys = [ 'name', 'command', 'services' ]
steps_optional_keys = [ 'location' ]
step_services_mandatory_keys = [ 'name' ]
step_services_optional_keys = [ 'datamovers' ]


# TODO: avoid raising HTTPException from the utils files:
#       conceptually we might want to use utils everywhere,
#       and would not expect them to return an HTTPException
def remove_duplicates(key_list: List[str]) -> List[str]:
    """Removes duplicates from a list of strings

    Args:
        key_list (List[str]): list of strings to process

    Returns:
        List[str]: sorted list with only unique strings
    """
    return sorted(list(set(key_list)))


def find_duplicates(key_list: List[str]) -> List[str]:
    """Finds duplicates in a list of strings

    Args:
        key_list (List[str]): list of strings to process

    Returns:
        List[str]: sorted list with the duplicated strings
    """
    dups = []
    for i in key_list:
        if key_list.count(i) > 1:
            dups += [i]
    return sorted(list(set(dups)))


def validate_type(object_to_validate: Any,
                  expected_type: Any,
                  object_to_validate_str: str,
                  expected_type_str: str,
                  wdf: str) -> None:
    """Validates that an object is of a given type.

    Args:
        object_to_validate (Any): the object to validate
        expected_type (Any): the expected object type
        object_to_validate_str (str): the object name
        expected_type_str (str): the expected type name
        wdf (str): the WDF name

    Returns:
        None
        Raises HTTP exception if object is not of the expected type
    """
    if not isinstance(object_to_validate, expected_type):
        raise HTTPException(
            status_code = 404,
            detail = (f"{object_to_validate_str} should be declared as a "
                      f"{expected_type_str} in {wdf}")
        )


def validate_description(mkeys: List[str],
                         okeys: List[str],
                         pkeys: List[str]) -> str:
    """Validates a description in the workflow description file
    Applies to services, services attributes.

    Args:
        mkeys (List[str]): keys that are mandatory in the description
        okeys (List[str]): keys that are optional in the description
        pkeys (List[str]): keys that are present in the description

    Returns:
        str: the detailed error string (empty if no error)
    """
    error_str = ""
    # All accepted keys = mandatory one + optional ones
    all_keys = sorted(mkeys + okeys)
    actual_keys = sorted(pkeys + okeys)
    if all_keys != actual_keys:
        duplicates = find_duplicates(pkeys)
        missing = list(set(all_keys) - set(actual_keys))
        extra = list(set(actual_keys) - set(all_keys))
        if duplicates:
            error_str += f"Duplicate key(s) {duplicates} "
        if missing:
            error_str += f"Missing key(s) {missing} "
        if extra:
            error_str += f"Extra key(s) {extra} "
    return error_str


def missing_values(description: Dict[str, Any]) -> bool:
    """Check whether all keys have values in a dictionary.

    Args:
        description (Dict[str, Any]): the description dictionary to check

    Returns:
        bool: True if number of keys and number of values are not the same
    """
    return len(list(description.keys())) != len(list(description.values()))


def validate_workflow_global(wdf: str,
                             present_keys: List[str]) -> None:
    """Validates the first level keys of a workflow description file.

    Args:
        wdf (str): the WDF name
        present_keys (List[str]): the 1st level keys that are present in the WDF

    Returns:
        None

    Raises:
        HTTP exception on error
    """
    detailed_error = validate_description(wdf_mandatory_keys, wdf_optional_keys, present_keys)
    if len(detailed_error) != 0:
        detailed_error += f"in {wdf}"
        raise HTTPException(
            status_code = 404,
            detail = detailed_error
        )


def validate_workflow_part(wdf: str,
                           present_keys: List[str]) -> None:
    """Validates the workflow part of a workflow description file.

    Args:
        wdf (str): the WDF name
        present_keys (List[str]): the keys that are present in the workflow part of the WDF

    Returns:
        None

    Raises:
        HTTP exception on error
    """
    detailed_error = validate_description(workflow_mandatory_keys,
                                          workflow_optional_keys,
                                          present_keys)
    if len(detailed_error) != 0:
        detailed_error += f"in {wdf}"
        raise HTTPException(
            status_code = 404,
            detail = detailed_error
        )


def is_valid_file_name(fname: str) -> Tuple[bool, str]:
    """Checks whether a strings is a valid file name.

    Args:
        fname (str): the string to check

    Returns:
        Tuple[bool, str]:
        - True if OK
        - if failure: the error message to be output by the caller
    """
    if len(fname) == 0:
        msg = "should not be empty"
        return False, msg

    if '/' in fname:
        msg = "should not contain a '/'"
        return False, msg

    return True, ''


def is_valid_session_name(session_name: str) -> Tuple[bool, str]:
    """Checks whether a strings is a valid session name.

    Args:
        session_name (str): the string to check

    Returns:
        Tuple[bool, str]:
        - True if OK
        - if failure: the error message to be output by the caller
    """
    # The session name is used in fine to build the service name.
    # And the service name will be used to build the name of the sbatch script
    # that starts the ephemeral service (SBB or GBF cases).
    # This means that the session name should not contain a '/'
    # that is a forbidden character in file names.
    return is_valid_file_name(session_name)


def is_valid_service_name(service_name: str) -> Tuple[bool, str]:
    """Checks whether a strings is a valid service name.

    Args:
        service_name (str): the string to check

    Returns:
        Tuple[bool, str]:
        - True if OK
        - if failure: the error message to be output by the caller
    """
    # The service name will be used to build the name of the sbatch script
    # that starts the ephemeral service (SBB or GBF cases).
    # This means that this name should not contain a '/'
    # that is a forbidden character in file names.
    return is_valid_file_name(service_name)


def build_service_name(user_name: str, session_name: str, service_name: str) -> str:
    """Generates a service name and returns it

    Args:
        user_name (str): the user login
        session_name (str): the session name
        service_name (str): the service name as provided by the user

    Returns:
        str: the built service name
    """
    return f"{user_name}-{session_name}-{service_name}"


def build_run_id(session_name: str, time_ns: int) -> str:
    """Builds a run id to be used by IOI and returns it

    Args:
        session_name (str): the session name
        time_ns (int): the session start time (ns since EPOCH)

    Returns:
        str: the built run_id
    """
    start_date = datetime.fromtimestamp(time_ns // 1000000000)
    return f"{session_name}-{start_date.strftime('%Y-%m-%d_%H:%M:%S')}"


def update_service_name_in_array(snames: List[str], user_name: str, session_name: str) -> List[str]:
    """Updates all service names in a an array

    Args:
        snames (List[str]): the array of service names
        user_name (str): the user name
        session_name (str): the session name

    Returns:
        List[str]: the updated array of service names
    """
    for idx, item in enumerate(snames):
        snames[idx] = build_service_name(user_name, session_name, item)
    return snames


def update_service_name_in_workflow_description(wf_description: Dict[str, Any],
                                                user_name: str,
                                                session_name: str) -> Dict[str, Any]:
    """Updates all service names in a json workflow description

    Args:
        wf_description (Dict[str, Any]): the workflow description
        user_name (str): the user name
        session_name (str): the session name

    Returns:
        List[Dict[str, Any]]: the updated workflow description
    """
    # Note that the workflow description has already been validated, so we are sure of the
    # following:
    # - the 'services' section is present
    # - each service in the 'services' section has a 'name' entry
    # - the 'steps' section is present
    # - each step in the 'steps' section has a 'services' section reduced to 1 element
    # - each service in the 'services' section of each step has a 'name' entry
    for service in wf_description['services']:
        service['name'] = build_service_name(user_name, session_name, service['name'])

    for step in wf_description['steps']:
        if step['services']:
            step['services'][0]['name'] = build_service_name(user_name,
                                                             session_name,
                                                             step['services'][0]['name'])

    return wf_description


def multi_services_checks(wdf: str,
                          services_all: List[Dict[str, Any]],
                          job_manager_commands: CommandSettings) -> List[str]:
    """Does the validation that involves multiple services of the same type.

    Args:
        wdf (str): the WDF name
        services_all (List[Dict[str, Any]]): the list of services that are present in the WDF
        job_manager_command (CommandSettings): Job manager commands

    Returns:
        None

    Raises:
        HTTP exception on error
    """
    # process each list of services with the same type
    for stdtype in list(EPHEMERAL_SERVICES.keys()):
        cur_services = [service for service in services_all if service['type'].upper() == stdtype]
        ephemeral_service = EPHEMERAL_SERVICES[stdtype](job_manager_commands)
        detailed_error = ephemeral_service.check_multi_service(cur_services)
        if len(detailed_error) != 0:
            raise HTTPException(
                status_code = 404,
                detail = f"Services description in {wdf}: " + detailed_error
            )


def validate_services_part(wdf: str,
                           services: List[Dict[str, Any]],
                           job_manager_commands: CommandSettings) -> List[str]:
    """Validates the services part of a workflow description file.

    Args:
        wdf (str): the WDF name
        services (List[Dict[str, Any]]): the list of services that are present in WDF
        job_manager_command (CommandSettings): Job manager commands

    Returns:
        List[str]: the sorted list of defined service names

    Raises:
        HTTP exception on error
    """
    ###
    # structure of the services part in the WDF:
    #   wf_description['services'] = List (already checked in the calling routine)
    #   wf_description['services'][i] = Dict for each element in the list
    #   wf_description['services'][i]['attributes'] = Dict
    ###
    error_code = 404
    defined_services = []
    for service in services:
        validate_type(service, dict, 'services[i]', 'dictionary', wdf)
        detailed_error = validate_description(services_mandatory_keys,
                                              services_optional_keys,
                                              list(service.keys()))
        if len(detailed_error) != 0:
            detailed_error += f"in services description in {wdf}"
            raise HTTPException(
                status_code = error_code,
                detail = detailed_error
            )

        if missing_values(service):
            detailed_error = ("Some services keys are missing values in "
                             f"services description in {wdf}")
            raise HTTPException(
                status_code = error_code,
                detail = detailed_error
            )

        stype = service['type'].upper()
        # The attributes keys depend on the service type, so use the appropriate method to get them
        try:
            ephemeral_service = EPHEMERAL_SERVICES[stype](job_manager_commands)
        except KeyError as nokey:
            raise HTTPException(
                status_code = error_code,
                detail = f"Ephemeral service {stype} is not supported. Cannot start it."
            ) from nokey
        else:
            srv_attributes_mandatory_keys = ephemeral_service.get_mandatory_keys()
            logger.debug(f"mandatory keys = {srv_attributes_mandatory_keys}")
            srv_attributes_optional_keys = ephemeral_service.get_optional_keys()
            logger.debug(f"optional keys = {srv_attributes_optional_keys}")

        sname = service['name']

        # Validate the service name
        valid, detailed_error = is_valid_service_name(sname)
        if not valid:
            raise HTTPException(
                status_code = error_code,
                detail = f"Service name ({sname}) {detailed_error}"
            )


        # Validate the service attributes description
        validate_type(service['attributes'], dict, 'services[i][\'attributes\']', 'dictionary', wdf)
        detailed_error = validate_description(srv_attributes_mandatory_keys,
                                              srv_attributes_optional_keys,
                                              list(service['attributes'].keys()))
        if len(detailed_error) != 0:
            detailed_error += f" for service {sname} attributes in {wdf}"
            raise HTTPException(
                status_code = error_code,
                detail = detailed_error
            )

        if missing_values(service['attributes']):
            detailed_error = ("Some keys are missing values for service "
                             f"{sname} attributes in {wdf}")
            raise HTTPException(
                status_code = error_code,
                detail = detailed_error
            )

        # Now do the checks that are specific to each ephemeral service.
        # e.g.: check that mountpoint exists when service is NFS, in order
        # not to wait till we fail the BB creation.
        detailed_error = ephemeral_service.check_service_attributes(service['attributes'])
        if len(detailed_error) != 0:
            detailed_error += f" for service {sname} in {wdf}"
            raise HTTPException(
                status_code = error_code,
                detail = detailed_error
            )

        # Everything OK, build the list of defined services
        # (used later on to check for undefined services used by some steps)
        defined_services.append(sname)

    # Do any check that involves serveral services: for example several NFS services should not
    # have the same mountpoint
    multi_services_checks(wdf, services, job_manager_commands)

    return sorted(defined_services)

def get_paths_from_dasi_cfg_file(dasi_cfg_file: str) -> Tuple[List[str], str]:
    """Get the paths from roots attribute from a DASI configuration file.

    Args:
        dasi_cfg_file (str): path to the configuration file

    Returns:
        Tuple[List[str], str]: 
        - list of absolute paths and empty message on success
        - empty list and error message else
    """
    # Check that dasi_cfg file exists and its content is readable
    detailed_error = ""
    paths = []
    try:
        open(dasi_cfg_file, "r", encoding="utf-8")
    except OSError:
        detailed_error += f"Could not open file {dasi_cfg_file} for reading"
    else:
        dasi_settings = DASISettings.from_yaml(dasi_cfg_file)
        spaces = dasi_settings.spaces
        if len(spaces) != 1:
            detailed_error += ("Unsupported number of spaces attribute for DASI "
                                "configuration file, only one space is supported")
        else:
            for root in spaces[0].roots:
                error = check_isabspathname(root.path)
                if error:
                    detailed_error += f"DASI root path ({root.path}) {error}"
                else:
                    paths += [ root.path ]
            logger.debug(f"dasi_cfg_file {dasi_cfg_file} contains {paths} paths")
    if detailed_error:
        paths = []
    return paths, detailed_error


def update_service_attributes_in_workflow_description(wf_description: Dict[str, Any],
                                                      session_name: str) -> Dict[str, Any]:
    """Updates service attributes in a json workflow description

    Args:
        wf_description (Dict[str, Any]): the workflow description
        session_name (str): the session name

    Returns:
        Dict[str, Any]: the updated workflow description
    """
    # Note that the workflow description has already been validated, so we are sure of the
    # following:
    # - the 'services' section is present
    # - each service in the 'services' section has mandatory attributes
    error_code = 404
    detailed_error = ""
    for srv in wf_description['services']:
        stype = srv['type'].upper()
        if stype == 'DASI':
            logger.debug(f"Update attributes for {srv['name']} (type={stype})")
            dasi_cfg_file = srv['attributes']['dasiconfig']
            paths, detailed_error = get_paths_from_dasi_cfg_file(dasi_cfg_file) # type: ignore
            if len(paths) == 1:
                mountpoint = paths[0]
                srv['attributes']['mountpoint'] = mountpoint
                ns_filename = hashlib.sha256(mountpoint.encode('utf-8')).hexdigest()
                srv['attributes']['namespace'] = os.path.join(srv['attributes']['namespace'],
                                                              ns_filename)
            else:
                if len(paths) > 1:
                    detailed_error += ("Unsupported number of roots attribute for DASI "
                                      "configuration file, only one root is supported")
                detailed_error += f" for service in session {session_name}"
                raise HTTPException(
                    status_code = error_code,
                    detail = detailed_error
                    )
    return wf_description

def validate_single_step(wdf: str,
                         step: Dict[str, Any]) -> None:
    """Validates a single step description part of a workflow description file.

    Args:
        wdf (str): the WDF name
        step (Dict[str, Any]): the step to validate

    Returns:
        None

    Raises:
        HTTP exception on error
    """
    error_code = 404

    detailed_error = validate_description(steps_mandatory_keys,
                                          steps_optional_keys,
                                          list(step.keys()))
    if len(detailed_error) != 0:
        detailed_error += f"in steps description in {wdf}"
        raise HTTPException(
            status_code = error_code,
            detail = detailed_error
        )

    if missing_values(step):
        detailed_error = f"Some steps keys are missing values in steps description in {wdf}"
        raise HTTPException(
            status_code = error_code,
            detail = detailed_error
        )

    sname = step['name']

    # step services description

    validate_type(step['services'], list, 'steps[i][\'services\']', 'list', wdf)
    # In the current version only a single service is authorized per step
    if len(step['services']) > 1:
        raise HTTPException(
            status_code = error_code,
            detail = f"More than one service required for step {sname} in {wdf}"
        )

    for srv in step['services']:
        validate_type(srv, dict, 'steps[i][\'services\'][j]', 'dictionary', wdf)
        detailed_error = validate_description(step_services_mandatory_keys,
                                              step_services_optional_keys,
                                              list(srv.keys()))
        if len(detailed_error) != 0:
            detailed_error += f"for step {sname} services in {wdf}"
            raise HTTPException(
                status_code = error_code,
                detail = detailed_error
            )

        if missing_values(srv):
            detailed_error = f"Some keys are missing values for step {sname} services in {wdf}"
            raise HTTPException(
                status_code = error_code,
                detail = detailed_error
            )



def validate_steps_part(wdf: str,
                        steps: List[Dict[str, Any]]) -> None:
    """Validates the steps part of a workflow description file.

    Args:
        wdf (str): the WDF name
        steps (List[Dict[str, Any]]): the list of steps that are present in WDF

    Returns:
        None

    Raises:
        HTTP exception on error
    """
    ###
    # structure of the steps part in the WDF:
    #   wf_description['steps'] = List (already checked in the calling routine)
    #   wf_description['steps'][i] = Dict for each element in the list
    #   wf_description['steps'][i]['services'] = List (checked in validate_single_step())
    #   wf_description['steps'][i]['services'][j] = Dict for each element in the list
    #                                               (checked in validate_single_step())
    ###
    defined_steps = []
    for step in steps:
        # The following routines raise exceptions if an error occured
        validate_type(step, dict, 'steps[i]', 'dictionary', wdf)
        validate_single_step(wdf, step)

        # Everything OK, build the list of defined steps
        # (used right after to check for steps defined more than once)
        defined_steps.append(step['name'])

    defined_steps = sorted(defined_steps)
    if len(defined_steps) != len(set(defined_steps)):
        raise HTTPException(
            status_code = 404,
            detail = f"Some steps are redefined in {wdf}"
        )


def validate_used_services(wdf: str,
                           defined_services: List[str],
                           steps: List[Dict[str, Any]]) -> List[str]:
    """Validates the services used in the steps part of the workflow description file

    Args:
        wdf (str): the WDF name
        defined_service (List[str]): services names already declared in the services section
        steps (List[Dict[str, Any]]): the list of steps that are present in WDF

    Returns:
        List[str]: the sorted list of used service names

    Raises:
        HTTP exception on error
    """
    used_services = []
    # Get the list of services used in all the steps
    for step in steps:
        for service in step['services']:
            used_services.append(service['name'])

    # Check it is included in the defined services
    single_used_services = remove_duplicates(used_services)
    if not set(single_used_services).issubset(set(defined_services)):
        used_but_not_defined = set(single_used_services) - set(defined_services)
        used_but_not_defined_list = list(sorted(used_but_not_defined))
        raise HTTPException(
            status_code = 404,
            detail = f"Some services are used but not defined in {wdf}: {used_but_not_defined_list}"
        )
    return single_used_services


def replace_variables(input_string: str, replacements: Dict[str, str]) -> str:
    """Given a string and a replacement dictionary, replaces each occurence of the keys in the
    string by the key value in the dictionary

    Args:
        input_string (str): the string to process
        replacements (Dict[str, str]): the variables and their replacement values

    Returns:
        str: the updated string
    """
    logger.debug(f"input_string = {input_string}")
    logger.debug(f"replacements = {replacements}")

    if not replacements:
        return input_string

    output_string = input_string

    for var_name in replacements.keys():
        output_string = output_string.replace(var_name, replacements[var_name])
    logger.debug(f"output_string = {output_string}")
    return output_string


def replace_all_variables(input_string: str,
                          predefined_vars: Dict[str, str],
                          cmdline_vars: Dict[str, str]) -> str:
    """Given an input string (that can be either the workflow description file contents or
    a step command line) and 2 replacement dictionaries (the predefined vars and the cmdline
    provided vars), replaces all variables with their values.

    Args:
        input_string (str): the input_string
        predefined_vars (Dict[str, str]): the predefined variables and their replacement values
        cmdline_vars (Dict[str, str]): the private session level variables and
                                       their replacement values

    Returns:
        str: the updated input string
             Raises exception in case of predefined variable redefinition
    """
    all_vars = predefined_vars
    logger.debug(f"predefined variables values = {all_vars}")
    logger.debug(f"command line variables values = {cmdline_vars}")

    # 1st check that no predefined variable is redefined
    if not all_vars.keys().isdisjoint(cmdline_vars):
        status_code = 404
        msg = "Predefined variables should not be redefined on the command line"
        logger.error(f"{status_code} response because {msg}")
        raise HTTPException(
            status_code = status_code,
            detail = msg
        )

    # Update the input string with the following values:
    # 1. the predefined variables values:
    # 2. any <private> variable defined on the command line
    all_vars.update(cmdline_vars)
    output_string = replace_variables(input_string, all_vars)
    logger.debug(f"OUTPUT_STRING after replacing predefined variables = {output_string}")

    return output_string


def search_undefined_variables(input_string: str) -> List[str]:
    """Given an input string with all vars aleady replaced, search any remaining variable not
    defined.

    Args:
        input_string (str): the input string

    Returns:
        List[str]: the list of undefined variables
    """
    # We are looking for words that begin with an alpha letter ([^\W\d_]),
    # followed by any number of {alphanumeric or _} (\w*)
    return re.findall(r'{{ [^\W\d_]\w* }}', input_string)


def search_session_undefined_variables(workflow_description: str) -> List[str]:
    """Given the workflow description file contents with session-level vars aleady replaced,
    search any remaining session-level variable not defined.

    Args:
        workflow_description (str): the workflow description as read from the wdf and vars replaced

    Returns:
        List[str]: the list of undefined variables in the session description part
    """
    instep = False
    undefined_variables = []
    workflow_lines = workflow_description.split('\n')
    # TODO: when datamovers are supported, they shoule be excluded from the check too:
    # a datamover can contain variables in the "elements" and these variables may be
    # replaced at step level.
    for line in workflow_lines:
        if line != 'steps:':
            # The only place where undefined vars should remain in the step descriptions
            # is the step command part
            if not instep or ( instep and 'command:' not in line ):
                # We are looking for words that begin with an alpha letter ([^\W\d_]),
                # followed by any number of {alphanumeric or _} (\w*)
                undefined_variables += search_undefined_variables(line)
        else:
            instep = True
    return undefined_variables


def leave_if_session_undefined_variables(workflow_description: str) -> None:
    """If some undefined variables remain in the session description part, leave with error.

    Args:
        workflow_description (str): the workflow description as read from the wdf and vars replaced

    Returns:
        None
        Raises exception on failure
    """
    undefined_variables = search_session_undefined_variables(workflow_description)
    if undefined_variables:
        status_code = 404
        msg = f"Session part of the WDF contains undefined variables: {undefined_variables}"
        logger.error(f"{status_code} response because {msg}")
        raise HTTPException(
            status_code = status_code,
            detail = msg
        )


def reserve_resources(request: Dict[str, Any], resource_mgr: ResourcemanagerSettings) -> int:
    """Asks the resource manager to reserve the resources described in the request

    Args:
        request (Dict[str, Any]): the request body to be sent
        resource_mgr(ResourcemanagerSettings): settings for the resource manager

    Returns:
        int: 0 on success
             -1 on reservation failure
    """
    rm_name = resource_mgr.name.upper()
    logger.info(f"Looking for resource manager {rm_name}")
    try:
        resource_manager = RESOURCE_MANAGERS[rm_name](resource_mgr)
    except KeyError:
        logger.error(f"Resource manager {rm_name} is not supported.")
        return -1

    rc = resource_manager.reserve_resources(request)
    if rc == -1:
        logger.error(f"Reservation request to {rm_name} failed for service {request['name']}")

    return rc


def launch_ephemeral_service(srv: Dict[str, Any],
                             workflow_name: str,
                             run_id: str,
                             user_name: str,
                             job_manager_commands: CommandSettings,
                             resource_mgr: ResourcemanagerSettings) -> int:
    """Runs the command that will create an ephemeral service

    Args:
        srv (Dict[str, Any]): the service as described in the WDF
        workflow_name (str): the workflow this step is defined into
        run_id (str): the session name suffixed by its starting timestamp
        user_name (str): the calling user name
        job_manager_command (CommandSettings): Job manager commands
        resource_mgr (ResourcemanagerSettings): settings for the resource manager

    Returns:
        int: job_submission_cmd command return code
             0 on success
             -1 upon failure
             Raises an exception if service not supported.
    """
    stype = srv['type'].upper()
    try:
        ephemeral_service = EPHEMERAL_SERVICES[stype](job_manager_commands)
    except KeyError as nokey:
        raise HTTPException(
            status_code = 404,
            detail = f"Ephemeral service {stype} is not supported. Cannot start it."
        ) from nokey
    else:
        # 1st, fill in the resources specification request depending on the service type.
        # Then, get access to the resource manager to do a reservation
        # If the reservation request fails, do not go further
        request = ephemeral_service.fill_reservation_request(srv, user_name)
        if reserve_resources(request, resource_mgr) == -1:
            return -1

        return ephemeral_service.start(srv, workflow_name, run_id)


def async_launch_ephemeral_service(srv: Dict[str, Any],
                                   workflow_name: str,
                                   run_id: str,
                                   user_name: str,
                                   job_manager_commands: CommandSettings,
                                   resource_mgr: ResourcemanagerSettings) -> int:
    """Asynchronously runs the command that will create an ephemeral service

    Args:
        srv (Dict[str, Any]): the service as described in the WDF
        workflow_name (str): the workflow this step is defined into
        run_id (str): the session name suffixed by its starting timestamp
        user_name (str): the calling user name
        job_manager_command (CommandSettings): Job manager commands
        resource_mgr (ResourcemanagerSettings): settings for the resource manager

    Returns:
        int: JobID of the sbatch command
             Raises an exception if service not supported.
             0 upon failure
             -1 to indicate to the caller that no dependency option will be needed for the steps
    """
    stype = srv['type'].upper()
    try:
        ephemeral_service = EPHEMERAL_SERVICES[stype](job_manager_commands)
    except KeyError as nokey:
        raise HTTPException(
            status_code = 404,
            detail = f"Ephemeral service {stype} is not supported. Cannot start it."
        ) from nokey
    else:
        # 1st, fill in the resources specification request depending on the service type.
        # 1st get access to the resource manager to do a reservation
        # If the reservation request fails, do not go further
        request = ephemeral_service.fill_reservation_request(srv, user_name)
        if reserve_resources(request, resource_mgr) == -1:
            return 0

        return ephemeral_service.async_start(srv, workflow_name, run_id)


def start_ephemeral_service(sync_start: bool,
                            service: Dict[str, Any],
                            workflow_name: str,
                            run_id: str,
                            user_name: str,
                            job_manager_commands: CommandSettings,
                            resource_mgr: ResourcemanagerSettings) -> Tuple[bool, str]:
    """Starts an ephemeral service

    Args:
        sync_start (bool): if True, synchronously start the ephemeral service
        service (Dict[str, Any]): the service as described in the WDF
        workflow_name (str): the workflow this step is defined into
        run_id (str): the session name suffixed by its starting timestamp
        user_name (str): the calling user name
        job_manager_commands (CommandSettings): Job manager commands
        resource_mgr (ResourcemanagerSettings): settings for the resource manager

    Returns:
        Tuple[bool, str]:
            - whether the start failed (True if the start failed)
            - the jobid of the start job: it will be used to build the dependency option
              when starting the steps.
              If we are in a synchronous start: returns -1. No dependency option is needed
              in this case.
            - the new service status
    """
    if sync_start:
        cmd_rc = launch_ephemeral_service(service, workflow_name, run_id, user_name,
                                          job_manager_commands, resource_mgr)
        jobid = -1
        failure = (cmd_rc != 0)
        service_status = ServiceStatus.ALLOCATED.value
    else:
        jobid = async_launch_ephemeral_service(service, workflow_name, run_id, user_name,
                                               job_manager_commands, resource_mgr)
        # The above command returns the jobid of the sbatch command if successful, 0 else
        failure = (jobid == 0)
        service_status = ServiceStatus.WAITING.value
    return failure, jobid, service_status


def stop_ephemeral_service(stype: str,
                           sname: str,
                           sjobid: int,
                           partition: str,
                           workflow_name: str,
                           run_id: str,
                           job_manager_commands: CommandSettings) -> int:
    """Runs the command that will stop an ephemeral service

    Args:
        stype (str): type of the service to stop (presently only "SBB")
        sname (str): name of the service to stop
        sjobid (int): the service starting job jobid
        partition (str): name of the partition where to run the command
        workflow_name (str): the workflow this step is defined into
        run_id (str): the session name suffixed by its starting timestamp
        job_manager_commands(CommandSettings): job manager commands

    Returns:
        int: srun command return code
    """
    try:
        ephemeral_service = EPHEMERAL_SERVICES[stype](job_manager_commands)
    except KeyError as nokey:
        raise HTTPException(
            status_code = 404,
            detail = f"Ephemeral service {stype} is not supported. Cannot stop it."
        ) from nokey

    return ephemeral_service.stop(sname, sjobid, partition, workflow_name, run_id)


def async_stop_ephemeral_service(stype: str,
                                 sname: str,
                                 sjobid: int,
                                 partition: str,
                                 workflow_name: str,
                                 run_id: str,
                                 job_manager_commands: CommandSettings) -> int:
    """Runs the command that will stop an ephemeral service

    Args:
        stype (str): type of the service to stop (presently only "SBB")
        sname (str): name of the service to stop
        sjobid (int): the service starting job jobid
        partition (str): name of the partition where to run the command
        workflow_name (str): the workflow this step is defined into
        run_id (str): the session name suffixed by its starting timestamp
        job_manager_commands(CommandSettings): job manager commands

    Returns:
        int: sbatch command JobID
    """
    try:
        ephemeral_service = EPHEMERAL_SERVICES[stype](job_manager_commands)
    except KeyError as nokey:
        raise HTTPException(
            status_code = 404,
            detail = f"Ephemeral service {stype} is not supported. Cannot stop it."
        ) from nokey

    return ephemeral_service.async_stop(sname, sjobid, partition, workflow_name, run_id)


def unlock_namespace(wfm_db: WFMDatabase, namespace: str) -> None:
    """Given a namespace, unlocks it by removing it from the DB

    Args:
        wfm_db (WFMDatabase): the DB the namespaces will be stored into
        namespace (str): namespace to unlock

    Returns:
        None
    """
    wfm_db.delete_nslock(namespace)


def stop_services(wfm_db: WFMDatabase,
                  services: List[Dict[str, Any]],
                  sync_stop: bool,
                  workflow_name: str,
                  run_id: str,
                  job_manager_commands: CommandSettings) -> None:
    """Given a list of services, stops each ephemeral service.
    In this routine the list of services comes from launch_used_services() where
    it was custom built.

    Args:
        wfm_db (WFMDatabase): the DB the namespace is stored into (if there is one)
        services (List[Dict[str, Any]]): names and types of the services to stop
        sync_stop (bool): whether to synchronously stop the services
        workflow_name (str): the workflow this step is defined into
        run_id (str): the session name suffixed by its starting timestamp
        job_manager_commands(CommandSettings): job manager commands

    Returns:
        None
    """
    logger.debug(f"services to stop: {services}")
    for srvtostop in services:
        if sync_stop:
            stop_ephemeral_service(srvtostop['type'].upper(), srvtostop['name'], srvtostop['jobid'],
                                   srvtostop['location'], workflow_name, run_id,
                                   job_manager_commands)
        else:
            async_stop_ephemeral_service(srvtostop['type'].upper(), srvtostop['name'],
                                         srvtostop['jobid'], srvtostop['location'],
                                         workflow_name, run_id, job_manager_commands)
        if len(srvtostop['namespace']) > 0:
            unlock_namespace(wfm_db, srvtostop['namespace'])


def check_and_lock_namespaces(wfm_db: WFMDatabase,
                              services: List[Dict[str, Any]]) -> str:
    """Given a list of services, checks and locks the namespaces potentially used by these services
    In this routine the list of services comes from the wdf description.

    Args:
        wfm_db (WFMDatabase): the DB the namespaces will be stored into
        services (List[Dict[str, Any]]): services to check and lock the NS for

    Returns:
        "" if successful
        Error message else
    """
    undo_list = []
    for service in services:
        namespace = service['attributes'].get('namespace')
        if namespace:
            # We have a namespace in the attributes
            # Check it is not locked by another service
            srv_list = wfm_db.get_services_from_ns(namespace)
            if len(srv_list) != 0:
                # Already locked - undo what has already been done and leave with error
                error_msg = f"NS {namespace} already used by other services {srv_list}"
                logger.info(error_msg)
                for undo_ns in undo_list:
                    wfm_db.delete_nslock(undo_ns)
                return error_msg
            else:
                # Not there yet, lock it
                wfm_db.add_nslock(namespace, service['name'])
                logger.info(f"Added NS {namespace} used by service {service['name']}")
                undo_list.append(namespace)
    return ""


def launch_used_services(wfm_db: WFMDatabase,
                         wf_description: Dict[str, Any],
                         used_services: List[str],
                         run_id: str,
                         sync_start: bool,
                         user_name: str,
                         job_manager_commands: CommandSettings,
                         resource_mgr: ResourcemanagerSettings) -> List[Dict[str, Any]]:
    """Given a list of used services, launch them all. For the moment
    only process the SBB, GBF and DASI services.

    Args:
        wfm_db (WFMDatabase): the DB the namespaces will be stored into
        wf_description (Dict[str, Any]): the workflow description
        used_services (List[Dict[str, Any]]): names of the services to start
        run_id (str): the session name suffixed by its starting timestamp
        sync_start (bool): whether to synchronously start the services
        user_name (str): the calling user name
        job_manager_commands(CommandSettings): job manager commands
        resource_mgr (ResourcemanagerSettings): settings for the resource manager

    Returns:
        List[Dict[str, Any]]: list of info related to the actually running services
        Raises exception in case of failure
    """
    logger.info(f"Launching the used services: {used_services}")
    workflow_name = wf_description['workflow']['name']

    # Check that namespaces are not used by another session and lock them.
    # Locking is achieved by storing the ns with the service name in the NS DB.
    # Note that the service name contains the session name
    error_msg = check_and_lock_namespaces(wfm_db, wf_description['services'])
    if len(error_msg) > 0:
        raise HTTPException(
            status_code = 404,
            detail = error_msg
        )

    # Used to save the running services information.
    running_services = []
    for service in wf_description['services']:
        logger.debug(f"================== SERVICE = {service} ======================")
        if service['name'] in used_services:
            failure, jobid, service_status = start_ephemeral_service(sync_start,
                                                                     service,
                                                                     workflow_name,
                                                                     run_id,
                                                                     user_name,
                                                                     job_manager_commands,
                                                                     resource_mgr)
            if failure:
                # If the service that just failed to start has an associated namespace,
                # unlock it.

                namespace = service['attributes'].get('namespace')
                if namespace:
                    logger.debug(f"UNLOCKING NAMESPACE {namespace}")
                    unlock_namespace(wfm_db, namespace)                    
                # Stop all the services we already launched for this workflow.
                # Stop them with the same synchronocity as the start.
                stop_services(wfm_db,
                              running_services[::-1],
                              sync_start,
                              workflow_name,
                              run_id,
                              job_manager_commands)

                # Raise exception
                raise HTTPException(
                    status_code = 404,
                    detail = (f"Failed to start {service['type']} service {service['name']} "
                               "- All services stopped")
                )

            # Everything OK
            # Save the service info to be able to stop that service in case of failure
            # and to return it in case of success.
            location = ''
            if 'location' in service['attributes'].keys():
                location = service['attributes']['location']

            namespace = ''
            if 'namespace' in service['attributes'].keys():
                namespace = service['attributes']['namespace']

            running_services.append({'name': service['name'],
                                     'type': service['type'].upper(),
                                     'namespace': namespace,
                                     'status': service_status,
                                     'jobid': jobid,
                                     'location': location})
    return running_services


def store_running_services(wfm_db: WFMDatabase,
                           services: List[Dict[str, Any]],
                           running_services: List[Dict[str, Any]]) -> None:
    """Given a list of services, store each one of them if it used by a step and running.

    Args:
        wfm_db (WFMDatabase): the DB the service will be stored into
        services (List[Dict[str, Any]]): the services as described in the workflow description
        running_services (List[Dict[str, Any]]): the services that are actually running

    Returns:
    """
    # Note that the order used to describe the services in the wdf (i.e. in services[]) is preserved
    # inside running_services.
    index = 0
    for service in services:
        if any(srv['name'] == service['name'] for srv in running_services):
            # Service successfully started and running: store it in the services table DB
            logger.info(f"Adding service to the DB: {{ {service['name']}:{service['type']}:"
                        f"{running_services[index]['jobid']} }}")
            wfm_db.add_unique_service(srv=service,
                                      srv_start=time.time_ns(),
                                      srv_status=running_services[index]['status'],
                                      srv_jobid=running_services[index]['jobid'])
            index += 1


def get_ephemeral_service_status_from_rm(srv: Dict[str, Any],
                                         job_manager_commands: CommandSettings) -> str:
    """Runs the command that will get an ephemeral service status.

    Args:
        srv (Dict[str, Any]): the service as described in the WDF
        job_manager_commands(CommandSettings): job manager commands

    Returns:
        str: service status as returned by scontrol
             empty string upon failure
    """
    stype = srv['type'].upper()
    try:
        ephemeral_service = EPHEMERAL_SERVICES[stype](job_manager_commands)
    except KeyError:
        logger.info(f"Ephemeral service type {stype} is not supported. "
                     "Cannot get its status from RM.")
        return ""

    sname = srv['name']
    status = ephemeral_service.get_service_status(sname)
    logger.info(f"Ephemeral service {sname} status = {status}")
    return status


def update_service_status_from_rm(wfm_db: WFMDatabase,
                                  srv: Dict[str, Any],
                                  job_manager_commands: CommandSettings) -> None:
    """Updates a service status in the DB after getting the actual ephemeral
    service status from the Resource Manager.

    Args:
        wfm_db (WFMDatabase): the DB the service is stored into
        srv (Dict[str, Any]): the service as described in the WDF
        job_manager_commands(CommandSettings): job manager commands

    Returns:
        None
        Raises exception upon failure
    """
    status = get_ephemeral_service_status_from_rm(srv, job_manager_commands)
    sname = srv['name']
    if not status:
        # An empty status string means that the servie type is not supported.
        # Do not update the service status in that case.
        # This is for testing purposes.
        logger.info(f"Ephemeral service type {srv['type']} is not supported. "
                     "Cannot update its status.")
    elif status == ServiceStatus.UNKNOWN.value:
        # Note: the returned status may be 'UNKNOWN'.
        # An 'UNKNOWN' status string means that the RM command (scontrol) failed
        # for some reason.
        # Do not update the service status in that case.
        logger.info(f"Ephemeral service {sname}: status=UNKNOWN. Cannot update its status.")
    else:
        logger.info(f"Update {sname} status to {status}")
        wfm_db.update_service_status(sname, status)


def update_services_status_from_rm(wfm_db: WFMDatabase,
                                   services: List[Dict[str, Any]],
                                   job_manager_commands: CommandSettings) -> None:
    """Given a list of services, updates each service status in the DB after
    getting the actual ephemeral service status from the Resource Manager.

    Args:
        wfm_db (WFMDatabase): the DB the service is stored into
        services (List[Dict[str, Any]]): the list of services to update
        job_manager_commands(CommandSettings): job manager commands

    Returns:
        None
    """
    for service in services:
        logger.debug(f"updating service {service['name']} status")
        update_service_status_from_rm(wfm_db, service, job_manager_commands)


def update_services_sessionid(wfm_db: WFMDatabase,
                              services: List[Dict[str, Any]],
                              session_id: int) -> None:
    """Given a list of service names, updates each service session id in the DB.

    Args:
        wfm_db (WFMDatabase): the DB the service is stored into
        services (List[Dict[str, Any]]): the list of services to update
        session_id(int): their session id

    Returns:
        None
    """
    for srv in services:
        wfm_db.update_service_sessionid(srv_name=srv['name'], srv_sessid=session_id)


def all_services_allocated(wfm_db: WFMDatabase,
                           session_id: int) -> bool:
    """Given a session id, checks that all its services are in the allocated state.
    The services status must have been updated by a call to the RM, prior to calling this routine.

    Args:
        wfm_db (WFMDatabase): the DB to get info from
        session_id (int): the session id

    Returns:
        bool: True if all the services are allocated - False else
    """
    services = wfm_db.get_services_info_from_session_id(session_id)
    # No service for this session is equivalent to "all services allocated",
    # since we want to allow steps to run w/o any service
    for service in services:
        if service['status'].upper() not in (ServiceStatus.ALLOCATED.value,
                                             ServiceStatus.STAGEDIN.value):
            return False
    return True


def all_services_stopped(wfm_db: WFMDatabase,
                         session_id: int) -> bool:
    """Given a session id, checks that all its services are in the stopped state.

    Args:
        wfm_db (WFMDatabase): the DB to get info from
        session_id (int): the session id

    Returns:
        bool: True if all the services are stopped - False else
    """
    # Get the services that belong to the session
    services = wfm_db.get_services_info_from_session_id(session_id)
    for service in services:
        if service['status'].upper() not in (ServiceStatus.STOPPED.value,
                                             ServiceStatus.STAGEDOUT.value):
            return False
    return True


def one_service_teardown(wfm_db: WFMDatabase, session_id: int) -> bool:
    """Given a session id, checks if one of its services is in the teardown state.

    Args:
        wfm_db (WFMDatabase): the DB to get info from
        session_id (int): the session id

    Returns:
        bool: True if one of the services is teardown - False else
    """
    # Get the services that belong to the session
    services = wfm_db.get_services_info_from_session_id(session_id)
    for service in services:
        if service['status'].upper() == (ServiceStatus.TEARDOWN.value):
            return True
    return False


def update_session_status_from_services(wfm_db: WFMDatabase,
                                        session_id: int,
                                        session_name: str,
                                        session_status: str,
                                        job_manager_commands: CommandSettings) -> None:
    """Given a session id, updates this session services if needed, from the RM.
    Also updates the session status if needed.

    Args:
        wfm_db (WFMDatabase): the DB where to search
        session_id (int): the session id
        session_name (str): the session name
        session_status (str): the session status
        job_manager_commands (CommandSettings): Job manager commands

    Returns:
        None
    """
    # Get the services that belong to that session
    services = wfm_db.get_services_info_from_session_id(session_id)
    if not services:
        # This session has no service associated.
        # Assuming this is correct, and to cover async start and stop:
        # - update its status to active if it was starting.
        # - update its status to stopped if it was stopping.
        if session_status.upper() == SessionStatus.STARTING.value:
            wfm_db.update_session_status(ses_name=session_name,
                                         ses_status=SessionStatus.ACTIVE.value)
        elif session_status.upper() == SessionStatus.STOPPING.value:
            wfm_db.update_session_status(ses_name=session_name,
                                         ses_status=SessionStatus.STOPPED.value)
        return

    update_services_status_from_rm(wfm_db, services, job_manager_commands)
    # If all the session services are now allocated, and the session was in the starting
    # state, update its status to active
    if all_services_allocated(wfm_db, session_id):
        if session_status.upper() == SessionStatus.STARTING.value:
            wfm_db.update_session_status(ses_name=session_name,
                                         ses_status=SessionStatus.ACTIVE.value)
        return

    # If all the session services are now stopped, and the session was in the stopping
    # state, update its status to stopped
    if (all_services_stopped(wfm_db, session_id) and
        session_status.upper() == SessionStatus.STOPPING.value):
            wfm_db.update_session_status(ses_name=session_name,
                                         ses_status=SessionStatus.STOPPED.value)

    # If some of the session services are teardown, update the session status to teardown
    # whatever its current state
    if (one_service_teardown(wfm_db, session_id)):
        wfm_db.update_session_status(ses_name=session_name,
                                     ses_status=SessionStatus.TEARDOWN.value)


def count_services_not_stopped(wfm_db: WFMDatabase,
                               sync_stop: bool,
                               services: List[Dict[str, Any]],
                               workflow_name: str,
                               run_id: str,
                               job_manager_commands: CommandSettings) -> int:
    """Counts the services that are not stopped, trying however to stop them
    if their status is appropriate.
    The services status must have been updated by a call to the RM, prior to calling this routine.

    Args:
        wfm_db (WFMDatabase): the DB where to search
        sync_stop (bool): whether to synchronously stop the services
        services (List[Dict[str, Any]]): the list of services the count is based on
        workflow_name (str): the workflow this step is defined into
        run_id (str): the session name suffixed by its starting timestamp
        job_manager_commands (CommandSettings): Job manager commands

    Returns:
        int: the number of services that are not in stopped state.
    """
    srv_not_stopped = 0

    for service in services:
        stype = service['type'].upper()
        sname = service['name']
        sstatus = service['status'].upper()
        # The used services should be in the allocated or staged-in or waiting state
        # to be "stoppable"
        if sstatus in (ServiceStatus.ALLOCATED.value,
                       ServiceStatus.STAGEDIN.value,
                       ServiceStatus.WAITING.value):
            # We need this state to manage the services that are asynchronously stopped:
            # if they are stopping, we should not try to stop them once more
            wfm_db.update_service_status(sname, ServiceStatus.STOPPING.value)
            logger.info(f"ABOUT TO STOP SERVICE {sname} (type {stype})")
            if sync_stop:
                cmd_rc = stop_ephemeral_service(stype=stype, sname=sname,
                                                sjobid=service['jobid'],
                                                partition=service['location'],
                                                workflow_name=workflow_name, run_id=run_id,
                                                job_manager_commands=job_manager_commands)
                stop_ok = (cmd_rc == 0)
            else:
                cmd_rc = async_stop_ephemeral_service(stype=stype, sname=sname,
                                                      sjobid=service['jobid'],
                                                      partition=service['location'],
                                                      workflow_name=workflow_name, run_id=run_id,
                                                      job_manager_commands=job_manager_commands)
                # The above routine returns the jobid of the sbatch command if successful, 0 else
                stop_ok = (cmd_rc != 0)
            if stop_ok:
                if sync_stop:
                    wfm_db.update_service_status(sname, ServiceStatus.STOPPED.value)
                    if 'namespace' in service.keys():
                        unlock_namespace(wfm_db, service['namespace'])
                else:
                    logger.info(f"Successfully submitted asynch stop of service {sname}")
                    srv_not_stopped += 1
            else:
                logger.error(f"Failed to stop service {sname}")
                srv_not_stopped += 1
        else:
            logger.info(f"SERVICE {sname} (type {stype}) is in status {sstatus} - NOT STOPPED")
            if sstatus not in (ServiceStatus.STOPPED.value, ServiceStatus.STAGEDOUT.value):
                srv_not_stopped += 1

    return srv_not_stopped


def session_exists(wfm_db: WFMDatabase,
                   sname: str,
                   wname: str) -> int:
    """Checks whether a session is already stored in the DB

    Args:
        wfm_db (WFMDatabase): the DB where to search
        sname (str): the session name
        wname (str): the owning workflow name

    Returns:
        int: 1 if the session is already stored, else 0
    """
    try:
        wfm_db.get_session_info_from_name(sname, wname)
    except UnexistingSessionNameError:
        return 0

    return 1


def leave_if_session_exists(wfm_db: WFMDatabase, sname: str, wname: str) -> None:
    """Checks whether a session is already stored in the DB and raises an exception if so.

    Args:
        wfm_db (WFMDatabase): the DB where to search
        sname (str): the session name
        wname (str): the owning workflow name

    Returns:
        None
        Raises exception on failure
    """
    if session_exists(wfm_db, sname, wname):
        status_code = 404
        msg = f"{sname} session (workflow {wname}) is already started"
        logger.error(f"{status_code} response because {msg}")
        raise HTTPException(
            status_code = status_code,
            detail = msg
        )


def add_step_descriptions(wfm_db: WFMDatabase,
                          steps: List[Dict[str, Any]],
                          session_id: int) -> None:
    """
    Given a list of step_descriptions, adds them to step_description table DB.

    Args:
        wfm_db (WFMDatabase): the DB where to search
        steps (List[Dict[str, Any]]): the step_descriptions list
        session_id (str): id of the session they belong to

    Returns:
        None
    """
    for step in steps:
        if len(step['services']) > 0:
            srv_name=step['services'][0]['name']
        else:
            srv_name = ""
        wfm_db.add_unique_step_description(step_sessid=session_id,
                                           step_name=step['name'],
                                           step_command=step['command'],
                                           service_name=srv_name)


def delete_all_session_steps_descriptions(wfm_db: WFMDatabase,
                                          session_id: int) -> None:
    """Deletes from the DB all steps descriptions that belong to a session

    Args:
        wfm_db (WFMDatabase): the DB where to search
        session_id (int): the session id

    Returns:
        None
    """
    step_descrs = wfm_db.get_step_descriptions_from_session_id(session_id)
    for step_descr in step_descrs:
        wfm_db.delete_step_description(step_descr['id'])


def get_session_list_if_unique(wfm_db: WFMDatabase,
                               session_name: str) -> List[Dict[str, Any]]:
    """Given a session name, checks whether there is a single session stored in the DB
    with this name and returns the singleton.

    Args:
        wfm_db (WFMDatabase): the DB where to search
        session_name (str): the session name

    Returns:
        List[Dict[str, Any]]: singleton containing the session info
                              Raises exception if error
    """
    try:
        session_list = wfm_db.get_session_info_from_name(session_name)
    except UnexistingSessionNameError as nosession:
        status_code = 404
        msg = f"Session {session_name} not stored in the WFM DB"
        logger.error(f"{status_code} response because {msg}")
        raise HTTPException(
            status_code = status_code,
            detail = msg
        ) from nosession
    logger.debug(f"session_list for SESSION {session_name} = {session_list}")

    # The session is supposed to be unique
    if len(session_list) != 1:
        status_code = 404
        msg = f"Session {session_name} is not unique in the WFM DB"
        logger.error(f"{status_code} response because {msg}")
        raise HTTPException(
            status_code = status_code,
            detail = msg
        )
    return session_list


def error_if_session_not_started(wfm_db: WFMDatabase,
                                 session: Dict[str, Any],
                                 job_manager_commands: CommandSettings) -> None:
    """
    Given a session item, leave with error if that session is not active.

    Args:
        wfm_db (WFMDatabase): the DB where to search
        session (Dict[str, Any]): the session item to check
        job_manager_command (CommandSettings): Job manager commands

    Returns:
        None
        Raises an exception if session is not active
    """
    if session['status'].upper() == SessionStatus.ACTIVE.value:
        return
    # Try to update the session status first.
    # This is because the services are started asynchronously using sbatch, so we need to
    # potentially update their state and the session state accordingly.
    update_session_status_from_services(wfm_db,
                                        session['id'],
                                        session['name'],
                                        session['status'],
                                        job_manager_commands)
    if session['status'].upper() != SessionStatus.ACTIVE.value:
        status_code = 404
        msg = f"Session {session['name']} not started yet"
        logger.error(f"{status_code} response because {msg}")
        raise HTTPException(
            status_code = status_code,
            detail = msg
        )


def setup_session_fields(wname: str, sname: str, status: str) -> Dict[str, Any]:
    """
    Given a set of strings, uses them to setup a session fields.

    Args:
        wname (str): the workflow name
        sname (str): the session name
        status (str): the session status

    Returns:
       The built session dictionary
    """
    cur_session = {
        'workflow_name': wname,
        'name': sname,
        'status': status,
        'steps': []
    }
    return cur_session


def setup_service_fields(services: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Given a list of services and depending of this list length, uses it to setup a services
    dictionary.

    Args:
        services (List[Dict[str, Any]]): the list of services

    Returns:
       The built service dictionary
    """
    # We are sure the list length is either 0 or 1, since the ids are unique in the DB
    if len(services) == 0:
        service_name = 'UNKNOWN'
        service_type = 'UNKNOWN'
        service_status = 'UNKNOWN'
        service_jobid = 0
    else:
        service_name = services[0]['name']
        service_type = services[0]['type'].upper()
        service_status = services[0]['status']
        service_jobid = services[0]['jobid']

    cur_service = {
        'name': service_name,
        'type': service_type,
        'status': service_status,
        'jobid': service_jobid
    }
    return cur_service


def combine_step_status_for_output(step_status: str,
                                   job_mgr: str,
                                   job_manager_commands: CommandSettings) -> str:
    """
    Given a step status potentially made of blank-sepatated status strings,
    combine them into a single status string aimed for output.
    This combination is job_mgr-dependent.

    Args:
        step_status (str): the step status
        job_mgr (str): the job manager we are using
        job_manager_commands (CommandSettings): job manager commands

    Returns:
        (str): The combined step status (always single string)
    """
    try:
        job_manager = JOB_MANAGERS[job_mgr](job_manager_commands)
    except KeyError:
        logger.warning("Job manager {job_mgr} unsupported - "
                      f"combined status = 1st status: {step_status.split()[0]}")
        return step_status.split()[0]

    logger.debug(f"Combine step status \"{step_status}\" through job manager {job_mgr}")
    return job_manager.combine_step_status_for_output(step_status)


def setup_steps_fields(stepd: Dict[str, Any],
                       step_list: List[Dict[str, Any]],
                       service: Dict[str, Any],
                       job_mgr: str,
                       job_manager_commands: CommandSettings) -> List[Dict[str, Any]]:
    """
    Given a service dictionary, a step description dictionary and a list of steps associated
    to this step description, uses all the parameters to setup another step list.
    This setup is done depending on the steps list length.

    Args:
        stepd (Dict[str, Any]): the step description
        step_list (List[Dict[str, Any]]): the step list
        service (Dict[str, Any]): the service
        job_mgr (str): the job manager we are using
        job_manager_commands (CommandSettings): job manager commands

    Returns:
       The built step list
    """
    if len(step_list) == 0:
        # No active step associated to this step description.
        # Fill in what we can with what we have in the step description:
        # - Use the step description name as the step name
        #   (otherwise it should be the step instance name)
        # - Set the command field
        steps = [ {
            'name': stepd['name'],
            'status': 'INACTIVE',
            'progress': "",
            'jobid': 0,
            'command': stepd['command'],
            'service': service
        } ]

    else:
        steps = []
        for step in step_list:
            current_status = combine_step_status_for_output(step['status'],
                                                            job_mgr,
                                                            job_manager_commands)
            steps += [ {
                'name': step['instance_name'],
                'status': current_status,
                'progress': step['progress'],
                'jobid': step['jobid'],
                'command': stepd['command'],
                'service': service
            } ]

    return steps


def process_heter_steps_status(step_list: List[Dict[str, Any]],
                               job_mgr: str,
                               job_manager_commands: CommandSettings) -> List[Dict[str, Any]]:
    """
    Given a list of steps, update their status for the steps that correspond to
    heterogenous jobs.

    Args:
        step_list (List[Dict[str, Any]]): the step list
        job_mgr (str): the job manager we are using
        job_manager_commands (CommandSettings): job manager commands

    Returns:
       The processed step list
    """
    for step in step_list:
        new_status = combine_step_status_for_output(step['status'],
                                                    job_mgr,
                                                    job_manager_commands)
        step['status'] = new_status
    return step_list


def generate_access_command(wfm_db: WFMDatabase,
                            session: Dict[str, Any],
                            job_manager_commands: CommandSettings) -> str:
    """
    Given a session item, generate the command that should be used to interactively
    access its services.

    Args:
        wfm_db (WFMDatabase): the DB where to search
        session (Dict[str, Any]): the session item we want to access to
        job_manager_command (CommandSettings): Job manager commands

    Returns:
        str: the command that should be used to interctively access session
             Raises an exception if session is not active
    """
    used_services =  wfm_db.get_services_info_from_session_id(session['id'])

    logger.debug(f"USED SERVICES = {used_services}")

    allocated_services = []
    for service in used_services:
        sstatus = service['status'].upper()
        # The used services should be in the allocated or staged-in state
        # in order they can be accessed
        if sstatus in (ServiceStatus.ALLOCATED.value, ServiceStatus.STAGEDIN.value):
            logger.debug(f"SERVICE {service['name']} (type {service['type']}) status {sstatus} "
                          "- CAN BE ACCESSED")
            allocated_services.append(service)
        else:
            logger.warning(f"SERVICE {service['name']} (type {service['type']}) status {sstatus} "
                            "- CANNOT BE ACCESSED")

    logger.debug(f"ALLOCATED SERVICES = {allocated_services}")

    if len(allocated_services) == 0:
        raise HTTPException(
            status_code = 404,
            detail = (f"No ephemeral service allocated for session {session['name']}. "
                      f"Cannot be accessed.")
        )

    # This is an implementation restriction: today we cannot do more than one "use_persistent"
    if len(allocated_services) > 1:
        raise HTTPException(
            status_code = 404,
            detail = "Accessing a session with more that 1 ephemeral service is not supported."
        )

    stype = allocated_services[0]['type'].upper()
    try:
        ephemeral_service = EPHEMERAL_SERVICES[stype](job_manager_commands)
    except KeyError as nokey:
        raise HTTPException(
            status_code = 404,
            detail = f"Ephemeral service {stype} is not supported. Cannot use it."
        ) from nokey

    return ephemeral_service.generate_use_cmd(allocated_services[0]['name'],
                                              allocated_services[0]['location'])


def run_step(wfm_db: WFMDatabase,
             step_name: str,
             step_command: str,
             workflow_name: str,
             run_id: str,
             service_id: int,
             job_manager_commands: CommandSettings) -> int:
    """Given a step description item, runs the associated step command, using the associated
    service.

    Args:
        wfm_db (WFMDatabase): the DB where to search
        step_name (str): the step name
        step_command (str): the step command
        workflow_name (str): the workflow this step is defined into
        run_id (str): the session name suffixed by its starting timestamp
        service_id (int): the service id
                          0 means no ephemeral service used
        job_manager_command (CommandSettings): Job manager commands

    Returns:
        int: The command return code
    """
    # service_id = 0 means that there is no ephemeral service, so that the command listed in
    # the step description should be run unchanged.
    # The NOEphemeralService class is used for that.
    if service_id == 0:
        logger.info(f"RUN command \"{step_command}\" without any service")
        ephemeral_service = EPHEMERAL_SERVICES['NONE'](job_manager_commands)
        return ephemeral_service.use("", 0, step_command, workflow_name, run_id)

    services = wfm_db.get_service_info_from_id(service_id)
    # We are sure the list length is either 0 or 1, since the ids are unique in the DB
    if not services:
        status_code = 404
        msg = f"Step {step_name} uses a service that is not stored in the DB"
        logger.error(f"{status_code} response because {msg}")
        raise HTTPException(
            status_code = status_code,
            detail = msg
        )

    service_type = services[0]['type'].upper()
    service_name = services[0]['name']
    service_jobid = services[0]['jobid']
    logger.debug(f"Step {step_name} uses service {service_name} of type {service_type} "
                 f"starter job jobid {service_jobid}")
    try:
        ephemeral_service = EPHEMERAL_SERVICES[service_type](job_manager_commands)
    except KeyError as nokey:
        status_code = 404
        msg = (f"Step {step_name} uses unsupported ephemeral service {service_name} "
               f"(type={service_type})")
        logger.error(f"{status_code} response because {msg}")
        raise HTTPException(
            status_code = status_code,
            detail = msg
        ) from nokey

    logger.info(f"RUN command \"{step_command}\" on service {service_name} type {service_type}")
    return ephemeral_service.use(service_name, service_jobid, step_command, workflow_name, run_id)


def stop_step(wfm_db: WFMDatabase,
              stepd_id: int,
              jobid: int,
              job_mgr: str,
              job_manager_commands: CommandSettings) -> int:
    """Stops a step given its id in the Step DB

    Args:
        wfm_db (WFMDatabase): the DB where to search
        stepd_id (int): the step description id this step is related to
        jobid (int): the job id to stop
        job_mgr (str) the job manager we are using
        job_manager_commands (CommandSettings): Job manager commands

    Returns:
        int: 0 on success
             1 else
    """
    stepds = wfm_db.get_step_description_from_id(stepd_id)
    if not stepds:
        logger.error(f"404 response because step description {stepd_id} is not stored in the DB")
        return 1

    try:
        job_manager = JOB_MANAGERS[job_mgr](job_manager_commands)
    except KeyError:
        logger.error(f"404 response because step {stepds[0]['name']} uses unsupported "
                     f"job manager {job_mgr}")
        return 1

    logger.info(f"Cancel jobid {jobid} through job manager {job_mgr}")
    return job_manager.cancel_job(jobid)


def get_rm_step_status(status: str, job_mgr: str, job_manager_commands: CommandSettings) -> str:
    """Given a job status as managed by the WFM, returns the corresponding status as
    managed by the RM.

    Args:
        status (str): the status to convert
        job_mgr (str) the job manager we are using
        job_manager_commands (CommandSettings): Job manager commands

    Returns:
        str: the converted status
             empty string if job manager not supported
    """
    try:
        job_manager = JOB_MANAGERS[job_mgr](job_manager_commands)
    except KeyError:
        logger.error(f"404 response because job manager {job_mgr} is not supported")
        # Identical value is returned for testing purposes
        return status
    return job_manager.to_rm_job_status(status)


def get_wfm_step_status(status: str, job_mgr: str, job_manager_commands: CommandSettings) -> str:
    """Given a job status as managed by the RM, returns the corresponding status as
    managed by the WFM.

    Args:
        status (str): the status to convert
        job_mgr (str) the job manager we are using
        job_manager_commands (CommandSettings): Job manager commands

    Returns:
        str: the converted status
             empty string if job manager not supported
    """
    try:
        job_manager = JOB_MANAGERS[job_mgr](job_manager_commands)
    except KeyError:
        logger.error(f"404 response because job manager {job_mgr} is not supported")
        return ""
    return job_manager.to_wfm_job_status(status)


def combine_step_status_for_stopping(step_status: str,
                                     job_mgr: str,
                                     job_manager_commands: CommandSettings) -> str:
    """
    Given a step status potentially made of blank-sepatated status strings,
    combine them into a single status string aimed for checking if the step can be stopped.
    This combination is job_mgr-dependent.

    Args:
        step_status (str): the step status
        job_mgr (str): the job manager we are using
        job_manager_commands (CommandSettings): job manager commands

    Returns:
        (str): The combined step status (always single string)
    """
    try:
        job_manager = JOB_MANAGERS[job_mgr](job_manager_commands)
    except KeyError:
        logger.warning("Job manager {job_mgr} unsupported - "
                      f"combined status = 1st status: {step_status.split()[0]}")
        return step_status.split()[0]

    logger.debug(f"Combine step status \"{step_status}\" through job manager {job_mgr}")
    return job_manager.combine_step_status_for_stopping(step_status)


def count_steps_not_stopped(wfm_db: WFMDatabase,
                            steps: List[Dict[str, Any]],
                            forced_stop: bool,
                            job_mgr: str,
                            job_manager_commands: CommandSettings) -> int:
    """Counts the steps that are not stopped, trying however to stop them if in "forced stop" mode.
    The steps status have been updated by a call to the RM, prior to calling this routine.

    Args:
        wfm_db (WFMDatabase): the DB where to search
        steps (List[Dict[str, Any]]): the list of steps the count is based on
        forced_stop (bool): whether): whether to stop any non stopped step
        job_mgr (str) the job manager we are using
        job_manager_commands (CommandSettings): Job manager commands

    Returns:
        int: the number of steps that are not in stopped state.
    """
    steps_not_stopped = 0
    for step in steps:
        current_status = combine_step_status_for_stopping(step['status'],
                                                          job_mgr,
                                                          job_manager_commands)
        wfm_status = get_wfm_step_status(current_status.upper(), job_mgr, job_manager_commands)
        if wfm_status != StepStatus.STOPPED.value:
            if forced_stop:
                retcode = stop_step(wfm_db,
                                    stepd_id=step['step_description_id'],
                                    jobid=step['jobid'],
                                    job_mgr=job_mgr,
                                    job_manager_commands=job_manager_commands)
                # We failed to stop the step: account for it in the steps not stopped
                if retcode != 0:
                    steps_not_stopped += 1
            else:
                steps_not_stopped += 1

    return steps_not_stopped


def get_session_step_from_name(wfm_db: WFMDatabase,
                               session_name: str,
                               step_name: str) -> List[Dict[str, Any]]:
    """Given a session name and a step name, returns the steps with that step
    name that belong to that session.

    Args:
        wfm_db (WFMDatabase): the DB where to search
        session_name (str): the session name
        step_name (str): the step name

    Returns:
        List[Dict[str, Any]]: List of steps
    """
    sessions = get_session_list_if_unique(wfm_db, session_name)
    logger.debug(f"sessions = {sessions}")
    logger.debug(f"get_step_description(session_id={sessions[0]['id']}, step_name={step_name})")
    step_descriptions = wfm_db.get_step_description(sessions[0]['id'], step_name)
    logger.debug(f"step_descriptions = {step_descriptions}")
    # We should have a single step with this step name for this session name
    if len(step_descriptions) != 1:
        status_code = 404
        msg = f"Step {step_name} not stored in the WFM DB for session {session_name}"
        logger.error(f"{status_code} response because {msg}")
        raise HTTPException(
            status_code = status_code,
            detail = msg
        )
    steps = wfm_db.get_steps_info_from_step_description_id(step_descriptions[0]['id'])
    logger.debug(f"steps = {steps}")
    return steps

def get_step_status_from_rm(jobid: int,
                            job_mgr: str,
                            job_manager_commands: CommandSettings) -> str:
    """Runs the command that will get a job status.

    Args:
        jobid (int): the job id to get status about
        job_mgr (str): the job manager we are using
        job_manager_commands(CommandSettings): job manager commands

    Returns:
        str: step status as returned by scontrol
             empty string upon failure
    """
    try:
        job_manager = JOB_MANAGERS[job_mgr](job_manager_commands)
    except KeyError:
        logger.warning(f"Job Manager {job_mgr} is not supported. Cannot get step status.")
        return ""

    status = job_manager.get_job_status(jobid)
    logger.debug(f"Job ({jobid}) status = {status} through job manager {job_mgr}")
    return status


def update_step_status_from_rm(wfm_db: WFMDatabase,
                               step: Dict[str, Any],
                               job_mgr: str,
                               job_manager_commands: CommandSettings) -> None:
    """Updates a step status in the DB after getting the actual job
    status from the Resource Manager.

    Args:
        wfm_db (WFMDatabase): the DB the step is stored into
        step (Dict[str, Any]): the step as described in the WDF
        job_mgr (str): the job manager we are using
        job_manager_commands(CommandSettings): job manager commands

    Returns:
        None
    """
    status = get_step_status_from_rm(step['jobid'], job_mgr, job_manager_commands)
    if not status:
        # An empty status string means that the servie type is not supported.
        # Do not update the service status in that case.
        # This is for testing purposes.
        logger.warning(f"Job manager {job_mgr} is not supported. Cannot update step status.")
    else:
        wfm_db.update_step_status(step['id'], status)


def get_updated_session_steps(wfm_db: WFMDatabase,
                              session_name: str,
                              stepd: Dict[str, Any],
                              job_mgr: str,
                              job_manager_commands: CommandSettings) -> List[Dict[str, Any]]:
    """Updates all steps status for a given session and step description and returns the list
    of updated steps.

    Args:
        wfm_db (WFMDatabase): the DB the steps are stored into
        session_name (str): The session name.
        stepd (Dict[str, Any]): The step description item.
        job_mgr (str): the job manager we are using
        job_manager_commands(CommandSettings): job manager commands

    Returns:
        List[Dict[str, Any]]:  List of steps with updated status
    """
    step_name = stepd['name']

    # Get all the steps related to this step description
    steps = wfm_db.get_steps_info_from_step_description_id(stepd['id'])
    logger.debug(f"Steps = {steps}")

    # if there is no step, no need to update their states, so this part can be bypassed
    if steps:
        for step in steps:
            # Next, update the step status according to the resource manager command
            update_step_status_from_rm(wfm_db, step, job_mgr, job_manager_commands)
    else:
        logger.info(f"No active step for {step_name} in session {session_name}")

    # Finally return the updated info
    return get_session_step_from_name(wfm_db, session_name, step_name)


def remove_ephemeral_services_files(sname: str,
                                    stype: str,
                                    job_manager_commands: CommandSettings) -> None:
    """Removes the temporary files used for the ephemeral services creation and destruction

    Args:
        sname (str): the service name
        stype (str): the service type
        job_manager_commands(CommandSettings): job manager commands

    Returns:
        None
    """
    logger.debug(f"Removing ephemeral service {sname} (type {stype}) files")
    try:
        ephemeral_service = EPHEMERAL_SERVICES[stype](job_manager_commands)
    except KeyError:
        logger.warning(f"Ephemeral service type {stype} is not supported. "
                        "Cannot remove its temporary files.")
        return

    ephemeral_service.remove_rm_temp_files(sname)


def finish_session_cleanup(wfm_db: WFMDatabase,
                           session_id: int,
                           session_name: str,
                           services: List[Dict[str, Any]],
                           steps: List[Dict[str, Any]],
                           job_manager_commands: CommandSettings) -> None:
    """Finish the cleanup of a session object by deleting all that needs to be deleted
    from the DB: services, steps, step descriptions and finally the session itself.

    Args:
        wfm_db (WFMDatabase): the DB the objects are stored into.
        session_id (str): The session id.
        session_name (str): The session name.
        services (List[Dict[str, Any]]): the services used by this session.
        steps (List[Dict[str, Any]]): The steps instanciated in this session.
        job_manager_commands(CommandSettings): job manager commands

    Returns:
        None
    """
    for service in services:
        wfm_db.delete_service(service['name'])
        remove_ephemeral_services_files(service['name'], service['type'].upper(),
                                        job_manager_commands)
        if 'namespace' in service.keys():
            unlock_namespace(wfm_db, service['namespace'])

    for step in steps:
        wfm_db.delete_step(step['id'])

    delete_all_session_steps_descriptions(wfm_db, session_id)

    wfm_db.update_session_status(session_name, SessionStatus.STOPPED.value)

    # Delete the session from the DB
    wfm_db.delete_session(session_id=session_id)

    logger.info(f"Finished session {session_name} cleanup")


def get_usable_locations(job_mgr: str,
                         job_manager_commands: CommandSettings,
                         resource_mgr: ResourcemanagerSettings) -> List[Dict[str, Any]]:
    """Returns the list of all locations available to the user.
    If there is a resource manager defined, it will process this request itself;
    Otherwise the request will be processed by the job manager.

    Args:
        job_mgr (str) the job manager we are using
        job_manager_command (CommandSettings): Job manager commands
        resource_mgr (ResourcemanagerSettings): settings for the resource manager

    Returns:
        List[Dict[str, Any]]: the list of available locations
        Raises HTTP exception if resource manager is not supported
    """
    rm_name = resource_mgr.name.upper()
    logger.info(f"Looking for resource manager {rm_name}")
    try:
        resource_manager = RESOURCE_MANAGERS[rm_name](resource_mgr)
    except KeyError as nokey:
        raise HTTPException(
            status_code = 404,
            detail = f"Resource manager {rm_name} is not supported. Unable to contact it."
        ) from nokey

    if rm_name == 'NONE':
        # No resource manager: get the info from the job manager
        logger.info(f"Resource manager = {rm_name}. Looking for job manager {job_mgr}")
        try:
            job_manager = JOB_MANAGERS[job_mgr](job_manager_commands)
        except KeyError as nokey:
            raise HTTPException(
                status_code = 404,
                detail = f"Job manager {job_mgr} is not supported."
            ) from nokey
        else:
            return job_manager.get_usable_locations()
    else:
        return resource_manager.get_usable_locations()


def get_usable_flavors(resource_mgr: ResourcemanagerSettings) -> List[Dict[str, Any]]:
    """Returns the list of all flavors available to the user.

    Args:
        resource_mgr (ResourcemanagerSettings): settings for the resource manager

    Returns:
        List[Dict[str, Any]]: the list of available flavors
        Raises HTTP exception:
        - if resource manager is not supported
        - if ephemeral service is not supported
    """
    rm_name = resource_mgr.name.upper()
    logger.info(f"Looking for resource manager {rm_name}")
    try:
        resource_manager = RESOURCE_MANAGERS[rm_name](resource_mgr)
    except KeyError as nokey:
        raise HTTPException(
            status_code = 404,
            detail = f"Resource manager {rm_name} is not supported."
        ) from nokey

    return resource_manager.get_usable_flavors()
