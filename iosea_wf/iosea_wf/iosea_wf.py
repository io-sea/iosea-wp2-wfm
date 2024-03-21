"""CLI part of the Workflow Manager.
"""
import sys
import os
import subprocess
from getpass import getuser
from loguru import logger
import httpx
import cloup
from cloup.constraints import mutually_exclusive
from cloup.constraints import require_all
from cloup.constraints import If, IsSet
from iosea_wf.config import WFM_CLI_CONFIG
from iosea_wf.config.settings import WFMCLISettings
from iosea_wf.utils.utils import process_get_request, convert_to_dict, process_start_session_request
from iosea_wf.utils.utils import output_flavors
from iosea_wf.utils.errors import VariableDefinitionSyntaxError

__copyright__ = """
Copyright (C) Bull S.A.S.
"""


#########
# iosea-wf start WORKFLOWFILE=<wf name>.yaml SESSION=<session name (unique by user)>
# iosea-wf stop  SESSION=<session name>
# iosea-wf status SESSION=<session name> [STEP=<step_name> [STEPID=<step_id>]]
# iosea-wf run SESSION=<session name> STEP=<step name (unique by user, session)>
#########


@cloup.group()
def cli() -> None:
    """Command line interface entry point

    Args:

    Returns:
        None
    """

@cloup.command(help='Starts a session')
@cloup.option('--settings', default=WFM_CLI_CONFIG, help='Path to the configuration file.')
@cloup.option('-S', '--syncstart', is_flag=True, default=False,
              help='Start the services synchronously')
@cloup.option('-w', '--workflowfile',
              type=cloup.Path('r'),
              required=True,
              help='Workflow Description file path.')
@cloup.option('-s', '--session', type=str, required=True, help='The Session name.')
@cloup.option('-d', '--define', type=str, multiple=True, help='A variable definition.')
def start(settings: str, syncstart: bool, workflowfile: str, session: str, define: str) -> int:
    """Starts a session from a workflow description file.

    Args:
        settings (str): path to the configuration file
        syncstart (bool): whether to start the services synchronously
        workflowfile (str): the workflow description file name.
        session (str): the session name.
        define (str): the variables and their replacement values.

    Returns:
        int: 0 on success - 1 else
    """
    cur_settings = WFMCLISettings.from_yaml(settings)
    logger.info(f"===================== STARTING SESSION {session} FROM WDF {workflowfile}")

    logger.info(f"variables definitions = {list(define)}")
    # This comes as a tuple:
    # ('var1=val1', 'var2=val2')
    # 1. Convert it to a list and check that each element in the tuple has a correct syntax
    # 2. Fill the replacement dictionary:
    #    key = '{{ varXXX }}'
    #    value = 'valXXX'
    try:
        replacement_dict = convert_to_dict(list(define))
    except VariableDefinitionSyntaxError as except_msg:
        logger.error(except_msg)
        print(except_msg)
        sys.exit(1)

    logger.debug(f"REPLACEMENT DICT = {replacement_dict}")

    cur_settings = WFMCLISettings.from_yaml(settings)
    api_base_url = (f"http://{cur_settings.server.host}:{cur_settings.server.port}"
                    f"{cur_settings.server.root_path}")

    start_session = api_base_url + "session/startup"
    workflow_description_file = os.path.abspath(workflowfile)

    # Check that workflow description file exists and get its content
    try:
        yfile = open(workflow_description_file, "r", encoding="utf-8")
    except OSError:
        logger.error(f"Could not open file {workflow_description_file} for reading")
        sys.exit(1)

    wf_description = yfile.read()
    yfile.close()

    # Before anything do a 1st check of the wdf yaml syntax
    cmdret = subprocess.run(['yamllint', '--no-warnings',
                             '-d', '{extends: default, rules: {line-length: disable}}',
                             workflow_description_file], capture_output=True)
    if cmdret.returncode != 0:
        logger.error(f"File {workflow_description_file} is not syntactically correct:")
        logger.error(f"{cmdret.stdout.decode('utf-8')}")
        logger.error("Please fix the errors before starting your session.")
        sys.exit(1)

    session_start_item = {'workflow_description_file': str(workflow_description_file),
                          'workflow_description': wf_description,
                          'sync_start': syncstart,
                          'session_name': str(session),
                          'user_name': getuser(),
                          'replacements': replacement_dict}

    retcode = process_start_session_request(url=start_session,
                                            session_start_struct=session_start_item,
                                            host=api_base_url)
    sys.exit(retcode)


@cloup.command(help='Stops a session')
@cloup.option('--settings', default=WFM_CLI_CONFIG, help='Path to the configuration file.')
@cloup.option('-S', '--syncstop', is_flag=True, default=False,
              help='Stops the services synchronously')
@cloup.option('-s', '--session', type=str, required=True, help='The Session name.')
@cloup.option('-f', '--force', is_flag=True, default=False, help='Whether to do a forced stop.')
def stop(settings: str, syncstop: bool, session: str, force: bool) -> int:
    """Stops a session.

    Args:
        settings (str): path to the configuration file
        syncstop (bool): whether to stop the services synchronously
        session (str): the session name.
        force (bool): whether to do a forced stop - defaults to False

    Returns:
        int: 0 on success - 1 else
    """
    cur_settings = WFMCLISettings.from_yaml(settings)
    logger.info(f"=== STOPPING SESSION {session} - settings in {settings}")
    api_base_url = (f"http://{cur_settings.server.host}:{cur_settings.server.port}"
                    f"{cur_settings.server.root_path}")

    if force:
        stop_session = api_base_url + "session/forcedstop"
    else:
        stop_session = api_base_url + "session/stop"
    logger.debug(f"post({stop_session}, sync_stop={syncstop}, session_name={session})")

    with httpx.Client(verify=False, timeout=60) as client:
        try:
            # Note that the json structure must be consistent with SessionStopItem() class in
            # wfm_api/wfm_api/models/session_metadata.py
            response = client.post(stop_session, json={'sync_stop': syncstop,
                                                       'session_name': str(session)})
            if response.status_code == httpx.codes.OK:
                if syncstop:
                    msg = f"Successfully stopped session {session}"
                    logger.info(msg)
                    print(msg)
                else:
                    msg = (f"Clean (iosea-wf status) the stopped session {session} "
                            "before reusing its name")
                    logger.info(msg)
                    print(msg)
                sys.exit(0)

        except httpx.ConnectError:
            logger.error("Cannot connect to WFM API "
                         f"({cur_settings.server.host}:{cur_settings.server.port})")
        else:
            logger.error(response.json()['detail'])
        sys.exit(1)


@cloup.command(help='Gets access to a session, using a given ephemeral service')
@cloup.option('--settings', default=WFM_CLI_CONFIG, help='Path to the configuration file.')
@cloup.option('-s', '--session', type=str, help='Get access to this session name.')
@cloup.option('-S', '--service', type=str, multiple=True, default=[],
              help='Get the access using these ephemeral services.')
def access(settings: str, session: str, service: str) -> int:
    """Starts a session from a workflow description file.

    Args:
        settings (str): path to the configuration file
        session (str): the session name.
        service (str): the ephemeral services to be used during the access.

    Returns:
        int: 0 on success - 1 else
    """
    cur_settings = WFMCLISettings.from_yaml(settings)

    logger.info(f"===================== GETTING ACCES TO SESSION {session}")

    services = list(service)
    if len(services) == 0:
        logger.info("SERVICES TO BE USED = ALL")
    else:
        logger.info(f"SERVICES TO BE USED = {services}")
        if len(services) > 1:
            logger.error("Using several services is not supported yet")
            sys.exit(1)

    api_base_url = \
        f"http://{cur_settings.server.host}:{cur_settings.server.port}{cur_settings.server.root_path}"

    access_url = api_base_url + "session/access"

    logger.debug(f"post({access_url}, json={{'session_name': {session}, 'services': {services} }}")

    with httpx.Client(verify=False, timeout=60) as client:
        try:
            # Note that the json structure must be coherent with SessionAccessItem() class in
            # wfm_api/wfm_api/models/session_metadata.py
            response = client.post(access_url,
                                   json={'session_name': session, 'services': services})
            if response.status_code == httpx.codes.OK:
                result = response.json()
                logger.debug(f"response = {result}")
                print(f"Type the following command in order to get access to session {session}:")
                print(f"      {result}")
                print("Then type ^D to exit")
                sys.exit(0)

        except httpx.ConnectError:
            logger.error("Cannot connect to WFM API "
                         f"({cur_settings.server.host}:{cur_settings.server.port})")
        else:
            logger.error(response.json()['detail'])
        sys.exit(1)


@cloup.command(help='Runs a step')
@cloup.option('--settings', default=WFM_CLI_CONFIG, help='Path to the configuration file.')
@cloup.option('-s', '--session', type=str, required=True, help='The Session name.')
@cloup.option('-t', '--step', type=str, required=True, help='The Step name.')
@cloup.option('-d', '--define', type=str, multiple=True, help='A variable definition.')
def run(settings: str, session: str, step: str, define: str) -> int:
    """Runs a step inside a session.

    Args:
        settings (str): path to the configuration file
        session (str): the session name.
        step (str): the step name.
        define (str): the variables and their replacement values.

    Returns:
        int: 0 on success - 1 else
    """
    cur_settings = WFMCLISettings.from_yaml(settings)
    logger.info(f"===================== RUNNING STEP {step} INSIDE SESSION {session}")

    logger.info(f"variables definitions = {list(define)}")
    # This comes as a tuple:
    # ('var1=val1', 'var2=val2')
    # 1. Convert it to a list and check that each element in the tuple has a correct syntax
    # 2. Fill the replacement dictionary:
    #    key = '{{ varXXX }}'
    #    value = 'valXXX'
    try:
        replacement_dict = convert_to_dict(list(define))
    except VariableDefinitionSyntaxError as except_msg:
        logger.error(except_msg)
        print(except_msg)
        sys.exit(1)

    logger.debug(f"REPLACEMENT DICT = {replacement_dict}")

    cur_settings = WFMCLISettings.from_yaml(settings)
    api_base_url = \
        f"http://{cur_settings.server.host}:{cur_settings.server.port}{cur_settings.server.root_path}"

    run_step = api_base_url + "step/startup"

    logger.debug(f"post({run_step}, json={{'session_name': {session}, 'step_name': {step}, "
                f"'replacements': {replacement_dict} }})")

    with httpx.Client(verify=False, timeout=60) as client:
        try:
            # Note that the json structure must be coherent with StepStartItem() class in
            # wfm_api/wfm_api/models/step_metadata.py
            response = client.post(run_step,
                                   json={'session_name': session,
                                         'step_name': step,
                                         'replacements': replacement_dict})
            if response.status_code == httpx.codes.OK:
                logger.info(f"Successfully started step {step} inside session {session}")
                print(f"Successfully submitted {step} step: {response.json()['instance_name']}")
                sys.exit(0)

        except httpx.ConnectError:
            logger.error("Cannot connect to WFM API "
                         f"({cur_settings.server.host}:{cur_settings.server.port})")
        else:
            logger.error(response.json()['detail'])
        sys.exit(1)


@cloup.command(help='Returns object status')
@cloup.option('--settings', default=WFM_CLI_CONFIG, help='Path to the configuration file.')
@cloup.option_group(
    "Status options",
    cloup.option('-a', '--allsessions',
                 is_flag=True, default=False, help='Get the status for all sessions.'),
    cloup.option('-s', '--session',
                 type=str, help='Get the status for this session name.'),
    cloup.option('-A', '--allservices',
                 is_flag=True, default=False, help='Get the status for all services.'),
    cloup.option('-S', '--service',
                 type=str, help='Get the status for this service name.'),
    constraint=mutually_exclusive
)
@cloup.option_group(
    "Session options",
    cloup.option('-s', '--session', type=str, help='Get the status for this session name.'),
    cloup.option('-t', '--step', type=str, help='Get the status for this step.'),
    constraint=If(IsSet('step'), then=require_all)
)
@cloup.option_group(
    "Session options",
    cloup.option('-s', '--session', type=str, help='Get the status for this session name.'),
    cloup.option('-T', '--allsteps', is_flag=True, default=False,
                 help='Get the status for all its steps.'),
    constraint=If(IsSet('allsteps'), then=require_all)
)
def status(settings: str,
           allsessions: bool,
           session: str,
           step: str,
           allsteps: bool,
           allservices: bool,
           service: str) -> None:
    """Prints an object status (session, service, step).

    Args:
        settings (str): path to the configuration file
        allsessions (bool): true if status is required for all sessions.
        session (str): the session name.
        step (str): the step name.
        allservices (bool): true if status is required for all services.
        service (str): the service name.

    Returns:
        None
    """
    cur_settings = WFMCLISettings.from_yaml(settings)
    api_base_url = (f"http://{cur_settings.server.host}:{cur_settings.server.port}"
                    f"{cur_settings.server.root_path}")
    connect_error_msg = ("Cannot connect to WFM API "
                         f"({cur_settings.server.host}:{cur_settings.server.port})")

    if allsessions:
        logger.info("===================== GETTING STATUS FOR ALL SESSIONS\n")
        endpoint = f"{api_base_url}session/all"
        msg = "No session found in the WFDB"
        err = False
        obj_wanted = 'session'

    elif session is not None:
        if allsteps:
            logger.info(f"===================== GETTING STATUS FOR ALL STEPS IN SESSION {session}")
            endpoint = f"{api_base_url}step/status/{session}"
            msg = f"Session {session} has no active step"
            err = False
            obj_wanted = 'step'

        elif step is None:
            logger.info(f"===================== GETTING STATUS FOR SESSION {session}\n")
            endpoint = f"{api_base_url}session/{session}"
            msg = f"Session {session} not found in the WFDB"
            err = True
            obj_wanted = 'session'

        else:
            logger.info(f"===================== GETTING STATUS FOR SESSION {session} - step {step}")
            endpoint = f"{api_base_url}step/status/{session}/{step}"
            msg = f"Step {step} not found in the WFDB for session {session}"
            err = True
            obj_wanted = 'step'

    elif allservices:
        logger.info("===================== GETTING STATUS FOR ALL SERVICES\n")
        endpoint = f"{api_base_url}service/all"
        msg = "No service found in the WFDB"
        err = False
        obj_wanted = 'service'

    elif service is not None:
        logger.info(f"===================== GETTING STATUS FOR SERVICE {service}\n")
        endpoint = f"{api_base_url}service/{service}"
        msg = f"Service {service} not found in the WFDB"
        err = True
        obj_wanted = 'service'

    # No parameter at all
    else:
        logger.info("===================== GETTING DETAILED STATUS FOR ALL SESSIONS\n")
        endpoint = f"{api_base_url}session/alldetailed"
        msg = "No session found in the WFM DB"
        err = False
        obj_wanted = 'all'

    try:
        process_get_request(endpoint, obj_wanted, msg, err, connect_error_msg)
    except KeyError:
        logger.error("Received incomplete status")


@cloup.command(help='Shows various configurations settings')
@cloup.option('--settings', default=WFM_CLI_CONFIG, help='Path to the configuration file.')
@cloup.option('-l', '--locations', is_flag=True, default=False,
              help='Shows the list of the configured MSA locations.')
@cloup.option('-f', '--flavors', is_flag=True, default=False,
              help='Show the list of the available flavors.')
def show(settings: str,
         locations: bool,
         flavors: bool) -> int:
    """Show configuration settings.

    Args:
        settings (str): path to the configuration file
        locations (bool): if true, show all possible locations
        flavors (bool): if true, show all possible flavors

    Returns:
        int: 0 on success - 1 else
    """
    cur_settings = WFMCLISettings.from_yaml(settings)

    logger.info("===================== SHOWING CONFIGURATION SETTINGS")

    api_base_url = (f"http://{cur_settings.server.host}:{cur_settings.server.port}"
                    f"{cur_settings.server.root_path}")

    if locations:
        config_url = f"{api_base_url}configuration/locations"
    else:
        config_url = f"{api_base_url}configuration/flavors"

    logger.debug(f"get({config_url})")

    with httpx.Client(verify=False, timeout=60) as client:
        try:
            response = client.get(config_url)
            if response.status_code == httpx.codes.OK:
                result_list = response.json()
                logger.debug(f"response = {result_list}")
                if locations:
                    print("Available partitions:")
                    for result in result_list:
                        print(f"{result['name']}", end=" ")
                    print()
                else:
                    output_flavors(result_list)
                sys.exit(0)

        except httpx.ConnectError:
            logger.error("Cannot connect to WFM API "
                         f"({cur_settings.server.host}:{cur_settings.server.port})")
        else:
            logger.error(response.json()['detail'])
        sys.exit(1)


@cloup.command(help='Updates a step', hidden=True)
@cloup.option('--settings', default=WFM_CLI_CONFIG, help='Path to the configuration file.')
@cloup.option('-j', '--jobid', type=str, required=True, help='The Step instance jobid.')
@cloup.option('-p', '--progress', type=str, required=True,
              help='The step new progress value (free string).')
def update(settings: str, jobid: int, progress: str) -> int:
    """Updates the job progress of a step instance.

    Args:
        settings (str): path to the configuration file
        jobid (str): the step instance jobid.
        progress (str): the new progress value.

    Returns:
        int: 0 on success - 1 else
    """
    logger.info(f"===================== UPDATING PROGRESS for JOBID {jobid}: \"{progress}\"")

    cur_settings = WFMCLISettings.from_yaml(settings)
    api_base_url = \
        f"http://{cur_settings.server.host}:{cur_settings.server.port}{cur_settings.server.root_path}"
    update_step_progress_url = api_base_url + "step/progress/job"

    step_progress_item = {'jobid': jobid, 'progress': progress}

    logger.debug(f"post({update_step_progress_url}, json={step_progress_item})")

    with httpx.Client(verify=False, timeout=60) as client:
        try:
            response = client.post(update_step_progress_url, json=step_progress_item)
            if response.status_code == httpx.codes.OK:
                msg = (f"Successfully updated progress for job {jobid} "
                       f"(step instance {response.json()})")
                logger.info(msg)
                print(msg)
                return 0

        except httpx.ConnectError:
            msg = ("Cannot connect to WFM API "
                  f"({cur_settings.server.host}:{cur_settings.server.port})")
            logger.error(msg)
            print(msg)
        else:
            print(response.json()['detail'])
            logger.error(response.json()['detail'])
        return 1


cli.add_command(start)
cli.add_command(run)
cli.add_command(stop)
cli.add_command(status)
cli.add_command(access)
cli.add_command(show)
cli.add_command(update)


if __name__ == '__main__':
    cli()
