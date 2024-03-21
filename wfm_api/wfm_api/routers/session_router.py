"""Contains endpoints for a session
"""

import time
from typing import Any, Dict, List
import yaml
from fastapi import APIRouter, Depends, HTTPException
from loguru import logger

from wfm_api.pax_hooks.wfm_database import wfm_database
from wfm_api.config.wfm_settings import WFMSettings
from wfm_api.models.session_metadata import SessionItem, SessionStartItem, SessionStopItem
from wfm_api.models.session_metadata import SessionAccessItem
from wfm_api.utils.database.wfm_database import WFMDatabase, SessionStatus
from wfm_api.utils.utils import validate_workflow_global, validate_workflow_part
from wfm_api.utils.utils import validate_services_part, validate_steps_part
from wfm_api.utils.utils import update_service_attributes_in_workflow_description
from wfm_api.utils.utils import validate_used_services, build_run_id, validate_type
from wfm_api.utils.utils import launch_used_services, store_running_services
from wfm_api.utils.utils import update_services_status_from_rm, count_services_not_stopped
from wfm_api.utils.utils import update_session_status_from_services, update_services_sessionid
from wfm_api.utils.utils import get_session_list_if_unique
from wfm_api.utils.utils import add_step_descriptions, finish_session_cleanup
from wfm_api.utils.utils import get_updated_session_steps, count_steps_not_stopped
from wfm_api.utils.utils import update_service_name_in_array, leave_if_session_exists
from wfm_api.utils.utils import update_service_name_in_workflow_description, is_valid_session_name
from wfm_api.utils.utils import replace_all_variables, leave_if_session_undefined_variables
from wfm_api.utils.utils import error_if_session_not_started, generate_access_command
from wfm_api.utils.utils import setup_session_fields, setup_service_fields, setup_steps_fields
from wfm_api.utils.errors import NoDocumentError, UnexistingSessionNameError

__copyright__ = """
Copyright (C) Bull S.A.S.
"""

session_router = APIRouter(prefix="/session", tags=["Session Data"])

def get_all_sessions_internal(wfm_db: WFMDatabase = Depends(wfm_database),
                              app_settings: WFMSettings = Depends(WFMSettings.provider)) -> List[Dict[str,Any]]:
    """Returns all sessions after updating their status

    Args:
        wfm_db (WFMDatabase): The workflow manager DB.
        app_settings (WFMSettings): The DB configuration settings.

    Returns:
        List[Dict[str,Any]]: List of sessions.
    """
    try:
        sessions = wfm_db.get_all_sessions()
    except NoDocumentError as exc:
        status_code=404
        msg = "No session found."
        logger.error(f"{status_code} response because {msg}")
        raise HTTPException(
            status_code=status_code,
            detail=msg
        ) from exc

    updated_sessions_list = []
    # Update the services and session status before actually returning the result
    for session in sessions:
        session_id = session['id']
        session_name = session['name']
        session_status = session['status']
        update_session_status_from_services(wfm_db,
                                            session_id,
                                            session_name,
                                            session_status,
                                            app_settings.command)
        try:
            cur_session_list = wfm_db.get_session_info_from_name(session_name)
        except UnexistingSessionNameError:
            logger.warning(f"Session {session_name} not in the database anymore")
        else:
            if cur_session_list[0]['status'].upper() == SessionStatus.STOPPED.value:
                # Since stopping the session services is done asynchronously,
                # we rely on the status command to finish the session cleanup from the
                # DB for any stopped session. Do it now since the session is in the
                # appropriate state. Moerover do not show this session in the final
                # session list.
                # Delete all that needs to be deleted from the DB:
                # services, steps, step descriptions and finally the session itself
                finish_session_cleanup(wfm_db,
                                       session_id,
                                       session_name,
                                       wfm_db.get_services_info_from_session_id(session_id),
                                       wfm_db.get_steps_info_from_session_id(session_id),
                                       app_settings.command)
            else:
                updated_sessions_list += cur_session_list
    return updated_sessions_list

@session_router.get("/all",
                    response_model=List[SessionItem],
                    response_model_by_alias=False,
                    response_model_exclude_unset=True,
                    summary="Get all sessions.")
async def get_all_sessions(wfm_db: WFMDatabase = Depends(wfm_database),
                           app_settings: WFMSettings = Depends(WFMSettings.provider)) -> List[Dict[str,Any]]:
    """Returns all sessions

    **Args:**\

    **Returns:**\
        `List[Dict[str,Any]]`: A response containing list of sessions.
    """
    return get_all_sessions_internal(wfm_db, app_settings)

@session_router.get("/alldetailed",
                    response_model=List[Dict[str,Any]],
                    response_model_by_alias=False,
                    response_model_exclude_unset=True,
                    summary="Get all sessions detailed info.")
async def get_all_sessions_detailed(wfm_db: WFMDatabase = Depends(wfm_database),
                                    app_settings: WFMSettings = Depends(WFMSettings.provider)) -> List[Dict[str,Any]]:
    """Returns all the details for all sessions

    **Args:**\

    **Returns:**\
        `List[Dict[str,Any]]`: A response containing list of sessions.
    """

    # Builds a list of dictionaries describing the sessions:

    # [
    #   {}, ...,
    #   {
    #     'workflow_name': 'string1', 'name': 'string2', 'status': 'string3',
    #     'steps': [
    #       {}, ...,
    #       {
    #         'name': 'string4', 'status': 'string5', 'jobid': 123, 'stepd_command': 'string6',
    #         'service':
    #           {'name': 'string7', 'type': 'string8', 'targets': 'strin9tmp',
    #            'status': 'strin10', 'jobid': 456},
    #       },
    #       ..., {}
    #     ]
    #   },
    #   ..., {}
    # ]

    session_list = []
    # This routine updates all sessions status as well as the services status when used by a session
    sessions = get_all_sessions_internal(wfm_db, app_settings)
    # For each session
    for session in sessions:
        # Begin filling in the current session dictionary
        cur_session = setup_session_fields(session['workflow_name'],
                                           session['name'],
                                           session['status'])
        cur_session['steps'] = []

        # Get the associated step descriptions
        step_descriptions = wfm_db.get_step_descriptions_from_session_id(session['id'])
        # Get all the steps related to each step description, with their status updated with the
        # actual status got from the RM.
        for stepd in step_descriptions:
            # Process the service associated to this step description
            services = wfm_db.get_service_info_from_id(stepd['service_id'])

            cur_service = setup_service_fields(services)

            # Get the steps associated to this step description
            step_list = get_updated_session_steps(wfm_db,
                                                  session['name'],
                                                  stepd,
                                                  app_settings.jobmanager.name,
                                                  app_settings.command)

            cur_session['steps'] += setup_steps_fields(stepd,
                                                       step_list,
                                                       cur_service,
                                                       app_settings.jobmanager.name,
                                                       app_settings.command)

        # Add this session description to the dictionary
        session_list += [ cur_session ]

    return session_list


# TODO the integration with keycloak has to be added
@session_router.get("/{session_name}",
                    response_model=List[SessionItem],
                    response_model_by_alias=False,
                    response_model_exclude_unset=True,
                    summary="Get sessions for a given session name.")
async def get_session(session_name: str,
                      wfm_db: WFMDatabase = Depends(wfm_database),
                      app_settings: WFMSettings = Depends(WFMSettings.provider)) -> List[Dict[str, Any]]:
    """Given a session name, returns the metadata associated with this session

    **Args:**\
        `session_name (str)`: The session name.
        `wfm_db (WFMDatabase)`: The workflow manager DB.
        `app_settings (WFMSettings)`: The DB configuration settings.

    **Returns:**\
        `List[SessionItem]`: A response containing list of sessions.
    """
    try:
        sessions = wfm_db.get_session_info_from_name(session_name)
    except UnexistingSessionNameError as exc:
        status_code = 404
        msg = f"No session with name {session_name}"
        logger.error(f"{status_code} response because {msg}")
        raise HTTPException(
            status_code=status_code,
            detail=msg
        ) from exc

    session_id = sessions[0]['id']
    session_status = sessions[0]['status']

    # Update the services and session status before actually returning the session info
    update_session_status_from_services(wfm_db,
                                        session_id,
                                        session_name,
                                        session_status,
                                        app_settings.command)
    try:
        cur_session_list = wfm_db.get_session_info_from_name(session_name)
    except UnexistingSessionNameError:
        logger.warning(f"Session {session_name} not in the database anymore")
    else:
        if cur_session_list[0]['status'].upper() == SessionStatus.STOPPED.value:
            # Since stopping the session services is done asynchronously,
            # we rely on the status command to finish the session cleanup from the
            # DB for any stopped session. Do it now since the session is in the
            # appropriate state. Moerover do not show this session in the final
            # session list.
            # Delete all that needs to be deleted from the DB:
            # services, steps, step descriptions and finally the session itself
            finish_session_cleanup(wfm_db,
                                   session_id,
                                   session_name,
                                   wfm_db.get_services_info_from_session_id(session_id),
                                   wfm_db.get_steps_info_from_session_id(session_id),
                                   app_settings.command)
            logger.warning(f"Session {session_name} not in the database anymore")
        else:
            return cur_session_list


# TODO the integration with keycloak has to be added
@session_router.post("/access",
                     response_model=str,
                     response_model_by_alias=False,
                     response_model_exclude_unset=True,
                     summary="Access a session through ephemeral services.")
async def access_session(
              item: SessionAccessItem,
              wfm_db: WFMDatabase = Depends(wfm_database),
              app_settings: WFMSettings = Depends(WFMSettings.provider)) -> str:
    """Given a session name and a set of services names, builds the command to access that
    session through these services and returns it to the CLI.

    **Args:**\
        `item` (SessionAccessItem): contains:
            `session_name (str)`: The session name to be accessed.
            `services (List[str])`: Ephemeral services to be used for this access.
                                    If empty list: use all the running services
                                    Currently: should be limited to 1

    **Returns:**\
        `command`: A response containing the command that should be typed to actually do the access.
    """
    session_name = item.session_name
    services = item.services

    if len(services) == 0:
        logger.info(f"ACCESSING SESSION {session_name} using all running services")
    else:
        logger.info(f"ACCESSING SESSION {session_name} using the services {services}")

    # Get the session list from its name - should be unique
    sessions = get_session_list_if_unique(wfm_db, session_name)
    logger.debug(f"session: {session_name} - status: {sessions[0]['status'].upper()}")

    # Leave if session is not started yet.
    error_if_session_not_started(wfm_db, sessions[0], app_settings.command)

    # Generate the command to access the services
    return generate_access_command(wfm_db, sessions[0], app_settings.command)


# TODO the integration with keycloak has to be added
@session_router.post("/startup",
                     response_model=List[SessionItem],
                     response_model_by_alias=False,
                     response_model_exclude_unset=True,
                     summary="Start a session from a workflow description file.")
async def start_session(
              item: SessionStartItem,
              wfm_db: WFMDatabase = Depends(wfm_database),
              app_settings: WFMSettings = Depends(WFMSettings.provider)) -> Dict[str, Any]:
    """Given a workflow description file, starts a session for this workflow

    **Args:**\
        `item` (SessionStartItem): contains:
            `workflow_description_file (str)`: The wdf pathname.
            `workflow_description (str)`: The wdf yaml str.
            `sync_start (bool)`: Whether to synchronously start the services.
            `session_name (str)`: The session name.
            `user_name (str)`: Login of the user that called the CLI.
            `replacements (Dict[str, str])`: Variables replacement values.

    **Returns:**\
        `SessionId`: A response containing the session id.
    """
    workflow_description_file = item.workflow_description_file
    workflow_description = item.workflow_description
    session_name = item.session_name
    sync_start = item.sync_start
    user_name = item.user_name
    replacements = item.replacements

    logger.info(f"STARTING SESSION {session_name} from WDF {workflow_description_file} "
                f"- sync = {str(sync_start)}")

    # The same check for the service name will be done later on, when processing
    # the services attributes.
    valid, msg = is_valid_session_name(session_name)
    if not valid:
        status_code = 404
        logger.error(f"{status_code} response because session name ({session_name}) {msg}")
        raise HTTPException(
            status_code=status_code,
            detail=f"session name ({session_name}) {msg}"
        )

    logger.debug(f"VARIABLES REPLACEMENTS {replacements}")

    # Update the workflow description with the following values:
    # 1. the predefined variables values:
    #    {{ SESSION }} = session name
    # 2. any <private> variable defined on the command line
    workflow_description = replace_all_variables(workflow_description,
                                                 { '{{ SESSION }}': session_name},
                                                 replacements)

    # After updating, if some undefined variables remain in the session description part,
    # leave with error.
    leave_if_session_undefined_variables(workflow_description)

    try:
        wf_description = yaml.safe_load(workflow_description)
    except yaml.YAMLError as exc:
        status_code = 404
        msg = f"Yaml parsing error in file {workflow_description_file}"
        if hasattr(exc, 'problem_mark'):
            mark = exc.problem_mark
            msg += f": {mark}"
        logger.error(f"{status_code} response because {msg}")
        raise HTTPException(
            status_code=status_code,
            detail=msg
        ) from exc

    ###
    # WDF structure:
    #   wf_description = Dict
    #   wf_description['workflow'] = Dict
    #   wf_description['services'] = List
    #   wf_description['services'][i] = Dict for each element in the list
    #   wf_description['services'][i]['attributes'] = Dict
    #   wf_description['steps'] = List
    #   wf_description['steps'][i] = Dict for each element in the list
    #   wf_description['steps'][i]['services'] = List
    #   wf_description['steps'][i]['services'][j] = Dict for each element in the list
    ###

    ###
    # Validation steps
    ###
    # 1st Validate the WDF description: it should be a dictionary
    # with all its mandatory keys present
    validate_type(wf_description, dict,
                  'workflow_description', 'dictionary',
                  workflow_description_file)
    validate_workflow_global(workflow_description_file,
                             list(wf_description.keys()))

    # 2nd Validate the Workflow description: it should be a dictionary
    validate_type(wf_description['workflow'], dict,
                  'workflow_description[\'workflow\']', 'dictionary',
                  workflow_description_file)
    validate_workflow_part(workflow_description_file,
                           list(wf_description['workflow'].keys()))

    # 3rd Validate the services descriptions: it should be a list of dictionaries.
    validate_type(wf_description['services'], list,
                  'workflow_description[\'services\']', 'list',
                  workflow_description_file)
    defined_services = validate_services_part(workflow_description_file,
                                              wf_description['services'],
                                              app_settings.command)

    # After validation update some service attributes in wf_description['services']
    wf_description = update_service_attributes_in_workflow_description(wf_description,
                                                                       session_name)

    # 4th Validate the steps descriptions: it should be a list of dictionaries.
    validate_type(wf_description['steps'], list,
                  'workflow_description[\'steps\']', 'list',
                  workflow_description_file)
    validate_steps_part(workflow_description_file, wf_description['steps'])

    # Get the list of services used in all the steps
    # after checking it is included in the defined services
    used_services = validate_used_services(workflow_description_file,
                                           defined_services,
                                           wf_description['steps'])

    workflow_name = wf_description['workflow']['name']

    # Check if session is already stored in the DB and leave if so.
    leave_if_session_exists(wfm_db, session_name, workflow_name)

    # After validation and before moving to launching the ephemeral services,
    # update the services names in the following structures:
    # - defined_services
    # - used_services
    # - wf_description['services']
    # - wf_description['steps']
    defined_services = update_service_name_in_array(defined_services, user_name, session_name)
    logger.debug(f"DEFINED SERVICES = {defined_services}")
    used_services = update_service_name_in_array(used_services, user_name, session_name)
    logger.debug(f"USED SERVICES = {used_services}")
    wf_description = update_service_name_in_workflow_description(wf_description, user_name,
                                                                 session_name)
    logger.debug(f"WF DESCRIPTION = {wf_description}")

    start_time=time.time_ns()
    run_id = build_run_id(session_name, start_time)

    # Launch all the used services
    # For the moment only process the SBB, GBF and DASI single services
    running_services = launch_used_services(wfm_db, wf_description, used_services, run_id,
                                            sync_start, user_name, app_settings.command,
                                            app_settings.resourcemanager)
    logger.debug(f"RUNNING SERVICES = {running_services}")

    store_running_services(wfm_db, wf_description['services'], running_services)

    # Everything went fine: add the session to the session table DB
    sessid = wfm_db.add_unique_session(ses_name=session_name,
                                       wkf_name=workflow_name,
                                       user_name=user_name,
                                       ses_start=start_time,
                                       ses_status=SessionStatus.STARTING.value)

    # Update the used services with the newly added session id
    update_services_sessionid(wfm_db, running_services, sessid)

    # Add the step_descriptions to step_description table DB.
    # Do it right now even if no step is launched, since we are parsing the WDF
    # and the session id is available from previous call.
    # Note that in the current version each step uses a single service.
    # This condition has already been checked above in validate_steps_part().
    add_step_descriptions(wfm_db, wf_description['steps'], sessid)

    # If the services are started asynchronously, do not update the session status right now.
    # The next get status command will do that.
    if sync_start:
        wfm_db.update_session_status(ses_name=session_name, ses_status=SessionStatus.ACTIVE.value)


def internal_stop_session(session_name: str,
                          force: bool,
                          sync_stop: bool,
                          wfm_db: WFMDatabase = Depends(wfm_database),
                          app_settings: WFMSettings = Depends(WFMSettings.provider)) -> int:
    """Given a session name, stops this session

    **Args:**\
        `session_name` (str): the name of the session to delete
        `force` (bool): whether to force the stop
        `sync_stop` (bool): whether to synchronously do the stop
        `wfm_db` (WFMDatabase): the DB all the objects are stored into

    **Returns:**\
        0: upon success

    **Raises:**\
        HTTP exception on error
    """
    ###
    # Validation steps
    ###
    try:
        session_list = wfm_db.get_session_info_from_name(session_name)
    except UnexistingSessionNameError as exc:
        status_code = 404
        msg = f"No session with name {session_name}"
        logger.error(f"{status_code} response because {msg}")
        raise HTTPException(
            status_code=status_code,
            detail=msg
        ) from exc

    logger.debug(f"STOPPING SESSION - session_list = {session_list}")
    if len(session_list) != 1:
        status_code = 404
        msg = f"Session {session_name} not unique"
        logger.error(f"{status_code} response because {msg}")
        raise HTTPException(
            status_code = status_code,
            detail = msg
        )

    # We should not have any "stopped" session in the DB
    # +
    # No need to stop an already stopping session
    cur_session_status = session_list[0]['status'].upper()
    if (cur_session_status in (SessionStatus.STOPPED.value, SessionStatus.STOPPING.value)
            and not force):
        status_code = 404
        msg = f"session {session_name} is already {cur_session_status}"
        logger.error(f"{status_code} response because {msg}")
        raise HTTPException(
            status_code = status_code,
            detail = msg
        )

    # current session status was saved above. Set it to "stopping"
    # to avoid starting other steps for this same session from another process
    wfm_db.update_session_status(session_name, SessionStatus.STOPPING.value)

    # Get the session id: will be used to search all the steps
    # and services attached to this session
    session_id = session_list[0]['id']
    logger.debug(f"STOPPING SESSION (session_id = {session_id})")

    # Get the steps after updating their status via the RM:
    # - if we are not in force mode, their jobs should all be stopped or
    #   the list of steps should be empty
    # - if we are in force mode, unconditionally do the job.
    stepds = wfm_db.get_step_descriptions_from_session_id(session_id)
    if not stepds:
        status_code = 404
        msg = f"no step description for session {session_name}"
        logger.error(f"{status_code} response because {msg}")
        raise HTTPException(
            status_code = status_code,
            detail = msg
        )
    steps = []
    for stepd in stepds:
        steps += get_updated_session_steps(wfm_db,
                                           session_name,
                                           stepd,
                                           app_settings.jobmanager.name,
                                           app_settings.command)

    # Count the steps that are not in the "stopped" status.
    # If there are some, give up the stop session unless we are in "forced stop" mode.
    # If all the steps are in the "stopped" status, delete them from the DB
    # once we are sure we can stop the session (i.e. after the services
    # have been successfully processed).
    # Note that the following routine stops any "non stopped" step if in "forced stop" mode.
    steps_not_stopped = count_steps_not_stopped(wfm_db, steps, force,
                                                app_settings.jobmanager.name,
                                                app_settings.command)

    if (not force) and (steps_not_stopped > 0):
        # Set the status to teardown: we need to be able to call stop
        # once more + we do not want any other start step to be issued.
        wfm_db.update_session_status(session_name, SessionStatus.TEARDOWN.value)
        status_code = 404
        msg = f"Session {session_name} has {steps_not_stopped} steps not yet completed"
        logger.error(f"{status_code} response because {msg}")
        raise HTTPException(
            status_code = status_code,
            detail = msg
        )

    # Get the services used by this session to update their state.
    used_services =  wfm_db.get_services_info_from_session_id(session_id)
    logger.debug(f"used_services : {used_services}")

    logger.debug("Updating used services status")
    update_services_status_from_rm(wfm_db, used_services, app_settings.command)

    # Get the services once more after their status has been updated.
    used_services =  wfm_db.get_services_info_from_session_id(session_id)

    run_id = build_run_id(session_name, session_list[0]['start_time'])
    # Count the used services that are not in the "stopped" state.
    # If there are some, give up the stop session.
    # Note that the following routine stops any "non stopped" service.
    srv_not_stopped = count_services_not_stopped(wfm_db,
                                                 sync_stop,
                                                 used_services,
                                                 session_list[0]['workflow_name'],
                                                 run_id,
                                                 app_settings.command)

    # Note that the force mode does not apply here: we absolutely need to ensure
    # the services are correctly stopped because of the amount of resources they
    # hold.
    # Note also that if we are in asynchronous mode, it is not an error if some services
    # are not stopped yet: the job in charge of stopping them may still be pending.
    if srv_not_stopped > 0:
        if sync_stop:
            # Stopping the services failed for some reason, but we want to be able
            # to stop it by other means or even by retrying later on.
            wfm_db.update_session_status(session_name, SessionStatus.TEARDOWN.value)
            status_code = 404
            msg = f"Session {session_name} has {srv_not_stopped} services not yet stopped"
            logger.error(f"{status_code} response because {msg}")
            raise HTTPException(
                status_code = status_code,
                detail = msg
            )
        else:
            # Just return: the status command will be in charge of cleaning everything
            # when it will be appropriate to do it.
            return 0

    # Once we are sure everything went fine, delete all that needs to be
    # deleted from the DB: services, steps, step descriptions and finally
    # the session itself
    finish_session_cleanup(wfm_db, session_id, session_name, used_services, steps,
                           app_settings.command)

    logger.info(f"Finished stopping session {session_name}")
    return 0


# TODO the integration with keycloak has to be added
@session_router.post("/stop",
                     response_model=int,
                     response_model_by_alias=False,
                     response_model_exclude_unset=True,
                     summary="Stop a session given its name.")
async def stop_session(item: SessionStopItem,
                       wfm_db: WFMDatabase = Depends(wfm_database),
                       app_settings: WFMSettings = Depends(WFMSettings.provider)) -> int:
    """Given a session name, stops this session

    **Args:**\
        `item` (SessionStopItem): contains:
            `sync_stop (bool)`: Whether to synchronously stop the services.
            `session_name (str)`: The session name to stop.

    **Returns:**\
        0: upon success

    **Raises:**\
        HTTP exception on error
    """
    logger.info(f"STOPPING SESSION {item.session_name} - sync_stop = {item.sync_stop}")
    return internal_stop_session(session_name=item.session_name, force=False,
                                 sync_stop=item.sync_stop,
                                 wfm_db=wfm_db, app_settings=app_settings)


# TODO the integration with keycloak has to be added
@session_router.post("/forcedstop",
                     response_model=int,
                     response_model_by_alias=False,
                     response_model_exclude_unset=True,
                     summary="Stop a session given its name.")
async def force_stop_session(item: SessionStopItem,
                             wfm_db: WFMDatabase = Depends(wfm_database),
                             app_settings: WFMSettings = Depends(WFMSettings.provider)) -> int:
    """Given a session name, unconditionally stops this session

    **Args:**\
        `item` (SessionStopItem): contains:
            `sync_stop (bool)`: Whether to synchronously stop the services.
            `session_name (str)`: The session name to stop.

    **Returns:**\
        0: upon success

    **Raises:**\
        HTTP exception on error
    """
    logger.info(f"FORCING STOP OF SESSION {item.session_name} - sync_stop = {item.sync_stop}")
    return internal_stop_session(session_name=item.session_name, force=True,
                                 sync_stop=item.sync_stop,
                                 wfm_db=wfm_db, app_settings=app_settings)
