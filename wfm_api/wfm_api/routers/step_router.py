"""Contains endpoints for a step
"""

from typing import Any, Dict, List
from fastapi import APIRouter, Depends, HTTPException
from loguru import logger

from pax.providers.oidc.provider import check_user
from pax.providers.oidc.models import UserClaims
from wfm_api.utils.errors import NoDocumentError
from wfm_api.pax_hooks.wfm_database import wfm_database
from wfm_api.config.wfm_settings import WFMSettings
from wfm_api.models.step_metadata import StepDescriptionItem, StepItem, StepStartItem
from wfm_api.models.step_metadata import StepProgressItem
from wfm_api.utils.database.wfm_database import WFMDatabase, StepStatus

from wfm_api.utils.utils import get_session_list_if_unique, get_updated_session_steps
from wfm_api.utils.utils import all_services_allocated, run_step, build_run_id
from wfm_api.utils.utils import replace_all_variables, search_undefined_variables
from wfm_api.utils.utils import error_if_session_not_started, get_rm_step_status
from wfm_api.utils.utils import process_heter_steps_status

__copyright__ = """
Copyright (C) Bull S. A. S.
"""

step_router = APIRouter(prefix="/step", tags=["Step Data"])

@step_router.get("/description/all",
                 response_model=List[StepDescriptionItem],
                 response_model_by_alias=False,
                 response_model_exclude_unset=True,
                 summary="Get all steps descriptions.")
async def get_all_steps_descriptions(
             wfm_db: WFMDatabase = Depends(wfm_database)) -> List[Dict[str,Any]]:
    """Returns all steps descriptions.

    **Args:**\

    **Returns:**\
        `List[StepDescriptionItem]`: A response containing list of steps descriptions..
    """
    try:
        return wfm_db.get_all_steps_descriptions()
    except NoDocumentError as exc:
        status_code = 404
        msg = "No step description found."
        logger.error(f"{status_code} response because {msg}")
        raise HTTPException(
            status_code=status_code,
            detail=msg
        ) from exc


# TODO the integration with keycloak have be added
@step_router.get("/description/{step_name}",
                 response_model=List[StepDescriptionItem],
                 response_model_by_alias=False,
                 response_model_exclude_unset=True,
                 summary="Get step description for a given step name.")
async def get_step_description(step_name: str,
                               wfm_db: WFMDatabase = Depends(wfm_database)) -> List[Dict[str, Any]]:
    """Given a step name, returns the metadata associated with this step's description

    **Args:**\
        `step_name (str)`: The step name.

    **Returns:**\
        `List[StepDescriptionItem]`: A response containing list of steps descriptions.
    """
    result = wfm_db.get_step_description_info_from_name(step_name)
    if not result:
        status_code = 404
        msg = f"No step description with name {step_name}"
        logger.error(f"{status_code} response because {msg}")
        raise HTTPException(
            status_code=status_code,
            detail=msg
        )

    return result


# TODO the integration with keycloak have be added
@step_router.get("/status/{session_name}/{step_name}",
                 response_model=List[StepItem],
                 response_model_by_alias=False,
                 response_model_exclude_unset=True,
                 summary="Get steps for given session and step names.")
async def get_step(session_name: str,
                   step_name: str,
                   wfm_db: WFMDatabase = Depends(wfm_database),
                   app_settings: WFMSettings = Depends(WFMSettings.provider)
                  ) -> List[Dict[str, Any]]:
    """Given a session name and a step name, returns the metadata associated with this step

    **Args:**\
        `session_name (str)`: The session name.
        `step_name (str)`: The step name.

    **Returns:**\
        `List[StepItem]`: A response containing list of steps.
    """
    # First get the session info
    try:
        sessions = get_session_list_if_unique(wfm_db, session_name)
    except HTTPException as session_except:
        raise HTTPException(
            status_code=session_except.status_code,
            detail=session_except.detail
        ) from session_except

    # Then get the step description for this step name
    step_descriptions = wfm_db.get_step_description(sessions[0]['id'], step_name)
    logger.debug(f"step_descriptions = {step_descriptions}")

    # We should have a single step description with this step name for this session name
    if not step_descriptions:
        status_code = 404
        msg = f"Step {step_name} not stored in the WFM DB for session {session_name}"
        logger.error(f"{status_code} response because {msg}")
        raise HTTPException(
            status_code = status_code,
            detail = msg
        )

    if len(step_descriptions) != 1:
        status_code = 404
        msg = f"Step {step_name} stored several times in the WFM DB for session {session_name}"
        logger.error(f"{status_code} response because {msg}")
        raise HTTPException(
            status_code = status_code,
            detail = msg
        )

    updated_step_list = get_updated_session_steps(wfm_db,
                                                  session_name,
                                                  step_descriptions[0],
                                                  app_settings.jobmanager.name,
                                                  app_settings.command)

    # If there are heterogenous jobs in the step list, update their status
    # Finally return the updated info
    return process_heter_steps_status(updated_step_list,
                                      app_settings.jobmanager.name,
                                      app_settings.command)


# TODO the integration with keycloak have be added
@step_router.get("/status/{session_name}",
                 response_model=List[StepItem],
                 response_model_by_alias=False,
                 response_model_exclude_unset=True,
                 summary="Get all steps for a given session name.")
async def get_all_session_steps(session_name: str,
                                wfm_db: WFMDatabase = Depends(wfm_database),
                                app_settings: WFMSettings = Depends(WFMSettings.provider)
                               ) -> List[Dict[str, Any]]:
    """Given a session name, returns the metadata associated with all steps in this session.

    **Args:**\
        `session_name (str)`: The session name.

    **Returns:**\
        `List[StepItem]`: A response containing list of steps.
    """
    # First get the session info
    try:
        sessions = get_session_list_if_unique(wfm_db, session_name)
    except HTTPException as session_except:
        raise HTTPException(
            status_code=session_except.status_code,
            detail=session_except.detail
        ) from session_except

    # Then get the step descriptions for this step name
    step_descriptions = wfm_db.get_step_descriptions_from_session_id(sessions[0]['id'])
    logger.debug(f"step_descriptions = {step_descriptions}")

    # If no step description for this session name, no need to continue
    if not step_descriptions:
        status_code = 404
        msg = f"No step description stored in the WFM DB for session {session_name}"
        logger.error(f"{status_code} response because {msg}")
        raise HTTPException(
            status_code = status_code,
            detail = msg
        )

    updated_step_list = []
    # Get all the steps related to this step description, with their status updated with the
    # actual status got from the RM.
    for stepd in step_descriptions:
        updated_step_list += get_updated_session_steps(wfm_db,
                                                       session_name,
                                                       stepd,
                                                       app_settings.jobmanager.name,
                                                       app_settings.command)

    if not updated_step_list:
        return []

    # If there are heterogenous jobs in the step list, update their status
    # Finally return the updated info
    return process_heter_steps_status(updated_step_list,
                                      app_settings.jobmanager.name,
                                      app_settings.command)


# TODO the integration with keycloak has to be added
@step_router.post("/startup",
                  response_model=Dict[str, str],
                  response_model_by_alias=False,
                  response_model_exclude_unset=True,
                  summary="Starts a step from a session.")
async def start_step(item: StepStartItem,
                     wfm_db: WFMDatabase = Depends(wfm_database),
                     app_settings: WFMSettings = Depends(WFMSettings.provider)) -> Dict[str, str]:
    """Given a session name and a step name, starts a step for this session

    **Args:**\
        `item` (StepStartItem): contains:
            `session_name (str)`: The owning session name.
            `step_name (str)`: The step name.
            `replacements (Dict[str, str])`: Variables replacement values.

    **Returns:**\
        `Dict[str,str]`: A response containing step id and instance name.
    """
    session_name = item.session_name
    step_name = item.step_name
    replacements = item.replacements

    logger.info(f"STARTING STEP {step_name} in SESSION {session_name}")

    # Get the session list from its name - should be unique
    sessions = get_session_list_if_unique(wfm_db, session_name)
    logger.debug(f"session: {session_name} - status: {sessions[0]['status'].upper()}")

    # Leave if session is not started yet.
    error_if_session_not_started(wfm_db, sessions[0], app_settings.command)

    session_id = sessions[0]['id']

    # Check that the services required by this session are allocated.
    # No need to update their status, it has already been done above.
    # TODO:
    # In a more elaborated version, we will check that all the services required
    # by this particular step are allocated.
    if not all_services_allocated(wfm_db, session_id):
        status_code = 404
        msg = f"Some services are not allocated for session {session_name}"
        logger.error(f"{status_code} response because {msg}")
        raise HTTPException(
            status_code = status_code,
            detail = msg
        )
    logger.debug(f"session: {session_name} - all services allocated")

    logger.debug(f"get_step_description(session_id={session_id}, step_name={step_name})")
    # Check that there is a step description inside the session for this step name.
    step_descriptions = wfm_db.get_step_description(session_id, step_name)
    logger.debug(f"STEP_DESCRIPTION={step_descriptions}")
    if len(step_descriptions) != 1:
        status_code = 404
        msg = (f"Step {step_name} description not stored in the Session DB for "
               f"sesssion {session_name}")
        logger.error(f"{status_code} response because {msg}")
        raise HTTPException(
            status_code = status_code,
            detail = msg
        )

    step_description_id = step_descriptions[0]['id']

    # Instantiate any step-level variable in the command part:
    # Update the step command description with the following values:
    # 1. the predefined variables values:
    #    {{ STEP }} = step name
    # 2. any <private> variable defined on the command line
    step_command = replace_all_variables(step_descriptions[0]['command'],
                                         { '{{ STEP }}': step_name},
                                         replacements)

    # After updating, if some undefined variables remain in the step command description part,
    # leave with error.
    undefined_variables = search_undefined_variables(step_command)
    if undefined_variables:
        status_code = 404
        msg = f"Step part of the step command undefined variables: {undefined_variables}"
        logger.error(f"{status_code} response because {msg}")
        raise HTTPException(
            status_code = status_code,
            detail = msg
        )

    # Add the step to the step table DB. We have to do this as soon as possible:
    # we need to set the step state to "starting" to avoid that another process
    # tries to run it at the same time.
    # STARTING is a special status managed internally, so we do not need to convert it through
    # get_rm_step_status()
    new_stepid, step_inst_name = wfm_db.add_step(step_description_id,
                                                 StepStatus.STARTING.value, "", step_command)

    logger.debug(f"session: {session_name} - new step {step_inst_name}")

    # TODO: the run_id could be the step instance_name
    run_id = build_run_id(sessions[0]['name'], sessions[0]['start_time'])
    # Actually run the step
    logger.debug(f"-> run_step(step_name={step_name},")
    logger.debug(f"            step_command={step_command},")
    logger.debug(f"            workflow_name={sessions[0]['workflow_name']},")
    logger.debug(f"            run_id={run_id},")
    logger.debug(f"            service_id={step_descriptions[0]['service_id']},")
    logger.debug(f"            sbatch_commands={app_settings.command})")

    jobid = run_step(wfm_db,
                     step_name=step_name,
                     step_command=step_command,
                     workflow_name=sessions[0]['workflow_name'],
                     run_id=run_id,
                     service_id=step_descriptions[0]['service_id'],
                     job_manager_commands=app_settings.command)
    if jobid == 0:
        # Something went wrong: delete the step
        wfm_db.delete_step(new_stepid)
        status_code = 404
        msg = f"Failed to run step {step_name}"
        logger.error(f"{status_code} response because {msg}")
        raise HTTPException(
            status_code = status_code,
            detail = msg
        )

    # Everything went fine:
    # Store the JobId into the step item
    # Also update the status (it was starting, its must now be active).
    wfm_db.update_step_jobid(step_id=new_stepid, jobid=jobid)

    # Add this step to the StepDescription
    wfm_db.update_step_description_with_step(stepd_id=step_description_id, step_id=new_stepid)
    # The above routine returns:
    # 0 on success
    # 1 if step description not found
    # 2 if step not found
    # We are sure none of the errors can occur, so no need to check

    # update the step status in the DB table
    step_status = get_rm_step_status(StepStatus.RUNNING.value,
                                     app_settings.jobmanager.name,
                                     app_settings.command)
    wfm_db.update_step_status(step_id=new_stepid, step_status=step_status)
    return {'id': new_stepid, 'instance_name': step_inst_name}


# TODO the integration with keycloak has to be added
@step_router.post("/progress/job",
                  response_model=str,
                  response_model_by_alias=False,
                  response_model_exclude_unset=True,
                  summary="Updates a step progress.")
async def update_step_progress(item: StepProgressItem,
                               wfm_db: WFMDatabase = Depends(wfm_database)) -> str:
    """Given a step jobid, updates the progress field for that step.

    **Args:**\
        `item` (StepProgressItem): contains:
            `jobid (int)`: The jobid that corresponds to the step to be updated.
            `progress (str)`: The new progress value.

    **Returns:**\
        str: the updated instance name upon success

    **Raises:**\
        HTTP exception on error
    """
    jobid = item.jobid
    progress = item.progress
    logger.debug(f"jobid={jobid} progress={progress}")

    steps = wfm_db.get_steps_info_from_jobid(jobid)
    if len(steps) == 0:
        status_code = 404
        msg = f"There is no active step for jobid {jobid}"
        logger.error(f"{status_code} response because {msg}")
        raise HTTPException(
            status_code = status_code,
            detail = msg
        )

    if len(steps) > 1:
        status_code = 404
        msg = "Too many active steps for jobid {jobid} ({len(steps)})"
        logger.error("{status_code} response because {msg}")
        raise HTTPException(
            status_code = status_code,
            detail = msg
        )

    logger.info(f"UPDATING PROGRESS FOR STEP {steps[0]['instance_name']} (jobid={jobid})")

    wfm_db.update_step_progress(step_id=steps[0]['id'], progress=f"{progress}")
    return steps[0]['instance_name']
