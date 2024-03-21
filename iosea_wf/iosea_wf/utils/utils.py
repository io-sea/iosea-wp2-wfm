"""Utility routines for the CLI part of the Workflow Manager.
"""
from typing import Any, Dict, List, Tuple

import operator
import httpx
from loguru import logger
from iosea_wf.utils.errors import VariableDefinitionSyntaxError

__copyright__ = """
Copyright (C) Bull S. A. S.
"""


def alert_missing_field(field: str) -> None:
    """Raises a KeyError exception when field is missing in the results.

    Args:
        field (str): the field name

    Returns:
        None : Raises an exception
    """
    logger.error(f"Result is incomplete: missing \'{field}\' field")
    raise KeyError(f"Result is incomplete: missing \'{field}\' field")


def check_missing_fields(results: List[Dict[str, Any]], fields: List[str]) -> None:
    """Given a status result, checks that all mandatory fields are present.

    Args:
        results (List[Dict[str, Any]): the status results
        fields (str): the list of mandatory fields

    Returns:
        None : Raises an exception
    """
    for result in results:
        for field in fields:
            cell = result.get(field)
            if cell is None:
                alert_missing_field(field)


def output_service_status_results(result: List[Dict[str, Any]]) -> None:
    """Outputs status results got after a get request (for a service)

    Args:
        result (List[Dict[str]): the status results

    Returns:
        None : Raises an exception upon failure
    """
    # First check that all the needed fields are present in the results.
    # This routine raises an exception in case of error, so no need to check a return code.
    check_missing_fields(result, ['name', 'status', 'type'])

    print(f"{'SERVICE': <50} {'TYPE': <10} {'STATUS': <20}")
    result.sort(key=operator.itemgetter('name'))
    for single_res in result:
        name = single_res.get('name')
        status = single_res.get('status')
        stype = single_res.get('type')
        print(f"{str(name): <50} {str(stype): <10} {str(status).lower(): <20}")


def output_session_status_results(result: List[Dict[str, Any]]) -> None:
    """Outputs status results got after a get request (for a session)

    Args:
        result (List[Dict[str]): the status results

    Returns:
        None : Raises an exception upon failure
    """
    # First check that all the needed fields are present in the results.
    # This routine raises an exception in case of error, so no need to check a return code.
    check_missing_fields(result, ['name', 'workflow_name', 'status'])

    print(f"{'SESSION': <50} {'WORKFLOW': <50} {'STATUS': <20}")
    result.sort(key=operator.itemgetter('name'))
    for single_res in result:
        workflow_name = single_res.get('workflow_name')
        name = single_res.get('name')
        status = single_res.get('status')
        print(f"{str(name): <50} {str(workflow_name): <50} {str(status).lower(): <20}")


def output_step_status_results(result: List[Dict[str, Any]]) -> None:
    """Outputs status results got after a get request for a step

    Args:
        result (List[Dict[str]): the status results

    Returns:
        None : Raises an exception upon failure
    """
    # First check that all the needed fields are present in the results.
    # This routine raises an exception in case of error, so no need to check a return code.
    check_missing_fields(result, ['id', 'instance_name', 'status', 'progress'])
    result.sort(key=operator.itemgetter('id'))
    print(f"{'ID': <10} {'INSTANCE': <40} {'STATUS': <15} {'JOBID': <10} {'PROGRESS': <20}")
    for single_res in result:
        step_id = single_res.get('id')
        instance_name = single_res.get('instance_name')
        status = single_res.get('status')
        job_id = single_res.get('jobid')
        progress = single_res.get('progress')
        # It is not necessarily an error not to have the jobid field
        # ex: if the step is starting and no job has been launched yet
        if not job_id:
            job_id = '-'
        print(f"{str(step_id): <10} {str(instance_name): <40} " +
              f"{str(status).lower(): <15} {str(job_id): <10} " +
              f"{str(progress): <20}")


def compute_max_len(results: List[Dict[str, Any]], field: str) -> int:
    """Given a list of dictionaries and a field name, computes the longest length corresponding
    to this field

    The results structure we look into is something like:
    [ { "field0": "value0_0", ..., "fieldN": "value0_N" },
      ...
      { "field0": "valueX_0", ..., "fieldN": "valueX_N" } ]

    The "field" param may be one the "fieldX" strings.

    Args:
        results (List[Dict[str, Any]]): the list of dictionaries to fetch
        field (str): the field to look for in the dictionaries

    Return:
        int: the longest length for the wanted field
    """
    field_len = 0
    for result in results:
        field_value = result.get(field)
        if field_value:
            field_len = max(field_len, len(str(field_value)))
    return field_len


def compute_second_level_max_len(initial_value: int,
                                 results: List[Dict[str, Any]],
                                 first_level_field: str,
                                 second_level_field:str) -> int:
    """Given a list of dictionaries and a field name, computes the longest length corresponding
    to this field inside a subdictionary

    The results structure we look into is something like:
    [ { "field0": "value0_0",
        ...,
        "fieldm": [ { "subf_m0": "subvm0_0_0", ..., "subf_mP": "subvm0_0_P" },
                    ...
                    { "subf_m0": "subvm0_Y_0", ..., "subf_mP": "subvm0_Y_P" },
                  ],
        ...,
        "fieldN": "value0_N" },
      ...
      { "field0": "valueX_0",
        ...,
        "fieldm": [ { "subf_m0": "subvmX_0_0", ..., "subf_mP": "subvmX_0_P" },
                    ...
                    { "subf_m0": "subvmX_Y_0", ..., "subf_mP": "subvmX_Y_P" },
                  ],
        "fieldN": "valueX_N" } ]

    The "first_level_field" param may be the "fieldm" string.
    The "second_level_field" param may be one the "subf_X" strings.

    Args:
        initial_value (int): initial value for the max length (usually the column header)
        results (List[Dict[str, Any]]): the list of dictionaries to fetch
        first_level_field (str): the field to get the sub-dictionary
        second_level_field (str): the field to look for in the dictionaries

    Return:
        int: the longest length for the wanted 2nd level field
    """
    max_field_len = initial_value
    for result in results:
        cur_field_len = compute_max_len(result[first_level_field], second_level_field)
        max_field_len = max(max_field_len, cur_field_len)
    return max_field_len


def compute_third_level_max_len(initial_value: int,
                                results: List[Dict[str, Any]],
                                first_level_field: str,
                                second_level_field: str,
                                third_level_field:str) -> int:
    """Given a list of dictionaries and a field name, computes the longest length corresponding
    to this field inside a dictionary in this subdictionary

    The results structure we look into is something like:
    [ { "field0": "value0_0",
        ...,
        "fieldm": [ { "subf_m0": "subvm0_0_0", ..., "subf_mP": {"FO": "V000", ..., "FA": "V00A"} },
                    ...
                    { "subf_m0": "subvm0_Y_0", ..., "subf_mP": {"FO": "V0Y0", ..., "FA": "V0YA"} },
                  ],
        ...,
        "fieldN": "value0_N" },
      ...
      { "field0": "valueX_0",
        ...,
        "fieldm": [ { "subf_m0": "subvmX_0_0", ..., "subf_mP": {"FO": "VX00", ..., "FA": "VX0A"} },
                    ...
                    { "subf_m0": "subvmX_Y_0", ..., "subf_mP": {"FO": "VXY0", ..., "FA": "VXYA"} },
                  ],
        "fieldN": "valueX_N" } ]

    The "first_level_field" param may be the "fieldm" string.
    The "second_level_field" param may be the "subf_mP" string.
    The "third_level_field" param may be one the "FX" strings.

    Args:
        initial_value (int): initial value for the max length (usually the column header)
        results (List[Dict[str, Any]]): the list of dictionaries to fetch
        first_level_field (str): the field to get the sub-dictionary
        second_level_field (str): the field to get the dictionary inside the sub-dictionary
        third_level_field (str): the field to look for in the dictionaries

    Return:
        int: the longest length for the wanted 3rd level field
    """
    max_field_len = initial_value
    for result in results:
        for subdict in result[first_level_field]:
            cur_val = subdict[second_level_field].get(third_level_field)
            if cur_val:
                max_field_len = max(max_field_len, len(str(cur_val)))
    return max_field_len


def output_detailed_results(results: List[Dict[str, Any]]) -> None:
    """Outputs detailed results (all objects) got after a get detailed request

    Args:
        result (List[Dict[str]): the status results

    Returns:
        None : Raises an exception upon failure
    """
    # First check that all the needed fields are present in the results.
    # This routine raises an exception in case of error, so no need to check a return code.
    check_missing_fields(results, ['workflow_name', 'name', 'status', 'steps'])
    for result in results:
        check_missing_fields(result['steps'], ['name', 'status', 'progress', 'command', 'service'])
        for step in result['steps']:
            service_list = [ step['service'] ]
            check_missing_fields(service_list, ['name', 'type', 'status'])

    # Compute the max length for each string field.
    # These lengths will be used to format the output
    session_name_len = max(len('SESSION'), compute_max_len(results, 'name'))
    workflow_name_len = max(len('WORKFLOW'), compute_max_len(results, 'workflow_name'))
    session_status_len = max(len('STATUS'), compute_max_len(results, 'status'))

    step_name_len = compute_second_level_max_len(len('STEP'), results, 'steps', 'name')
    step_status_len = compute_second_level_max_len(len('STATUS'), results, 'steps', 'status')
    step_progress_len = compute_second_level_max_len(len('PROGRESS'), results, 'steps', 'progress')
    # We will enclose the progress string between quotes
    step_progress_len += 2
    jobid_len = compute_second_level_max_len(len('JOBID'), results, 'steps', 'jobid')

    srv_name_len = compute_third_level_max_len(len('SERVICE'), results, 'steps', 'service', 'name')
    srv_type_len = compute_third_level_max_len(len('TYPE'), results, 'steps', 'service', 'type')
    srv_status_len = compute_third_level_max_len(len('STATUS'), results, 'steps', 'service',
                                                 'status')

    print(f"{'SESSION': <{session_name_len}} {'WORKFLOW': <{workflow_name_len}} "
          f"{'STATUS': <{session_status_len}} {'STEP': <{step_name_len}} "
          f"{'STATUS': <{step_status_len}} {'JOBID': <{jobid_len}} "
          f"{'PROGRESS': <{step_progress_len}} "
          f"{'SERVICE': <{srv_name_len}} {'TYPE': <{srv_type_len}} "
          f"{'STATUS': <{srv_status_len}} {'STEP COMMAND'}")

    for result in results:
        workflow_name = result.get('workflow_name')
        session_name = result.get('name')
        session_status = result.get('status')
        for step in result['steps']:
            step_name = step.get('name')
            step_status = step.get('status')
            job_id = step.get('jobid')
            step_progress = step.get('progress')
            if len(step_progress) == 0:
                step_progress = '-'
                sp_string = f"{str(step_progress): <{step_progress_len}}"
            else:
                sp_string = f"\"{str(step_progress)}\""
                sp_string = f"{sp_string: <{step_progress_len}}"
            command = step.get('command')
            # It is not necessarily an error not to have the jobid field
            # ex: if the step is starting and no job has been launched yet
            if not job_id:
                job_id = '-'
            srv_name = step['service'].get('name')
            srv_status = step['service'].get('status')
            srv_type = step['service'].get('type')
            print(f"{str(session_name): <{session_name_len}} "
                  f"{str(workflow_name): <{workflow_name_len}} "
                  f"{str(session_status).lower(): <{session_status_len}} "
                  f"{str(step_name): <{step_name_len}} "
                  f"{str(step_status).lower(): <{step_status_len}} {str(job_id): <{jobid_len}} "
                  f"{str(sp_string)} "
                  f"{str(srv_name): <{srv_name_len}} {str(srv_type): <{srv_type_len}} "
                  f"{str(srv_status).lower(): <{srv_status_len}} {str(command)}")


def output_results(result: List[Dict[str, Any]], obj_wanted: str, msg: str, err: bool) -> int:
    """Outputs results got after a get request

    Args:
        result (List[Dict[str]): the status results
        obj_wanted (str): the object we want results for
           'step': the results are for a step
           'service': the results are for a service
           'session': the results are for a session
           'all': the results are for a detailed output (all objects)
        msg (str): message to output in case of error
        err (bool): True if error, False if info

    Returns:
        int: 0 if result was not empty
             1 else
    """
    if result:
        if obj_wanted == 'step':
            output_step_status_results(result)
        elif obj_wanted == 'service':
            output_service_status_results(result)
        elif obj_wanted == 'session':
            output_session_status_results(result)
        else:
            output_detailed_results(result)
        return 0
    # No result
    if err:
        logger.error(msg)
    else:
        logger.info(msg)
        print(msg)
    return 1


def process_get_request(endpoint: str,
                        obj_wanted: str,
                        msg: str,
                        err: bool,
                        connect_msg: str) -> int:
    """Send a get request and output the answer

    Args:
        endpoint (str): the endpoint URL
        obj_wanted (str): the object we want results for
           'step': the results are for a step
           'service': the results are for a service
           'session': the results are for a session
           'all': the results are for a detailed output (all objects)
        msg (str): message to output in case of error
        err (bool): True if error, False if info
        connect_msg (str): message to output in case of connection error

    Returns:
        int: 0 upon success, 1 else
    """
    logger.info(f"get({endpoint})")
    with httpx.Client(verify=False, timeout=60) as client:
        try:
            rget = client.get(endpoint)
            if rget.status_code == httpx.codes.OK:
                result = rget.json()
                return output_results(result, obj_wanted, msg, err)

        except httpx.ConnectError:
            logger.error(connect_msg)
            return 1

        else:   # http request status not OK
            logger.error(rget.json()['detail'])
            return 1


def valid_string_format(definition: str) -> Tuple[bool, str, str]:
    """Check that the input string has a valid variable definition format: 'variable=value'

    Args:
        definition (str): the input string

    Returns:
        Tuple[bool, str, str]:
            bool: True if format is valid
            str: the key that should be stored in the dictionary
            str: the key value that should be stored in the dictionary
    """
    valid = True
    new_key = ''
    new_value = ''
    strings = definition.split('=')
    # Check the format is 'key=value'
    if len(strings) != 2:
        valid = False
    # check that neither the key, nor the value are empty
    elif (len(strings[0]) == 0) or (len(strings[1]) == 0):
        valid = False
    # Check the key begins with a letter
    elif strings[0][0].isalpha():
        # Check the reminder of the key has only alphanum chars and underscores
        if all(curc.isalnum() or curc =='_' for curc in strings[0][1:]):
             # Generate the dictionary
            new_key = f"{{{{ {strings[0]} }}}}"
            new_value = strings[1]
        else:
            logger.error("Left part of the definition should contain only alphanumeric "
                         f"chars or \'_\' : \'{definition}\'")
            valid = False
    else:
        logger.error("Left part of the definition should begin with a letter: "
                     f"\'{definition}\'")
        valid = False
    return valid, new_key, new_value


def convert_to_dict(definitions: List[str]) -> Dict[str, str]:
    """Convert a tuple of strings into a dictionary.
    The tuple has the format (... 'variable=value' ...)
    The generated dictionary has the format {... '{{ variable }}': 'value'...}

    Args:
        definitions (List[str]): the list of variables definitions

    Returns:
        Dict[str, str]: dictionary filled with the variables settings
        Raises: VariableDefinitionSyntaxError
    """
    out_dict = {}
    for definition in definitions:
        valid, new_key, new_value = valid_string_format(definition)
        if not valid:
            raise VariableDefinitionSyntaxError(definition)
        # Generate the dictionary
        out_dict[new_key] = new_value
    return out_dict


def process_start_session_request(url: str, session_start_struct: Dict[str, Any], host: str) -> int:
    """Send a post request to start a session

    Args:
        url (str): the endpoint URL
        session_start_struct (Dict[str, Any]): the session start structure to be sent in the request
            - workflow_description_file (str): the WDF
            - workflow_description (str): the WDF contents
            - sync_start (bool): whether to start the session synchronously
            - session_name (str): the session name
            - user_name (str): the caller lgin name
            - replacements (Dict[str, str]: the variables and their values as set on the cmd line
        host (str): host address for the message to output in case of connection error

    Returns:
        int: 0 upon success, 1 else
    """
    logger.info(f"post({url}, json={session_start_struct})")

    with httpx.Client(verify=False, timeout=60) as client:
        try:
            # Note that the json structure must be consistent with SessionStartItem() class in
            # wfm_api/wfm_api/models/session_metadata.py
            response = client.post(url, json=session_start_struct)
            if response.status_code == httpx.codes.OK:
                if session_start_struct['sync_start']:
                    msg = f"Successfully started session {session_start_struct['session_name']}"
                else:
                    msg = (f"Check session {session_start_struct['session_name']} status before "
                            "starting any step")

                logger.info(msg)
                print(msg)
                return 0

        except httpx.ConnectError:
            msg = f"Cannot connect to WFM API {host}"
        else:
            msg = str(response.json()['detail'])

        logger.error(msg)
        print(msg)
        return 1


def output_flavors(results: List[Dict[str, Any]]) -> None:
    """Outputs results got after a get flavors request

    Args:
        result (List[Dict[str]): the flavors descriptions

    Returns:
        None : Raises an exception upon failure
    """
    logger.debug(f"RESULTS = {results}")
    # First check that all the needed fields are present in the results.
    # This routine raises an exception in case of error, so no need to check a return code.
    check_missing_fields(results, ['name', 'cores', 'msize', 'ssize'])

    # Compute the max length for each string field.
    # These lengths will be used to format the output
    flavor_len = max(len('FLAVOR'), compute_max_len(results, 'name'))
    cores_len = max(len('CORES'), compute_max_len(results, 'cores'))
    msize_len = max(len('MEMORY SIZE'), compute_max_len(results, 'msize'))
    ssize_len = max(len('STORAGE SIZE'), compute_max_len(results, 'ssize'))

    print(f"{'FLAVOR': <{flavor_len}} {'CORES': <{cores_len}} "
          f"{'MEMORY SIZE': <{msize_len}} {'STORAGE SIZE': <{ssize_len}}")

    for result in results:
        flavor = result.get('name')
        cores = result.get('cores')
        if cores == 0:
            cores = '-'
        msize = result.get('msize')
        if msize == 0:
            msize = '-'
        ssize = result.get('ssize')
        if ssize == 0:
            ssize = '-'
        print(f"{str(flavor): <{flavor_len}} {str(cores): <{cores_len}} "
                  f"{str(msize): <{msize_len}} {str(ssize): <{ssize_len}}")
