"""Contains endpoints for a service
"""

from typing import Any, Dict, List
from fastapi import APIRouter, Depends, HTTPException
from loguru import logger

from pax.providers.oidc.provider import check_user
from pax.providers.oidc.models import UserClaims
from wfm_api.pax_hooks.wfm_database import wfm_database
from wfm_api.config.wfm_settings import WFMSettings
from wfm_api.utils.utils import update_service_status_from_rm
from wfm_api.utils.errors import NoDocumentError, UnexistingServiceNameError
from wfm_api.utils.database.wfm_database import WFMDatabase

__copyright__ = """
Copyright (C) Bull S. A. S.
"""

service_router = APIRouter(prefix="/service", tags=["Service Data"])

@service_router.get("/all",
                    response_model=List[Dict[str, Any]],
                    response_model_by_alias=False,
                    response_model_exclude_unset=True,
                    summary="Get all services.")
async def get_all_services(
              wfm_db: WFMDatabase = Depends(wfm_database),
              app_settings: WFMSettings = Depends(WFMSettings.provider)) -> List[Dict[str, Any]]:
    """Returns all services

    **Args:**\

    **Returns:**\
        `List[Dict[str, Any]]`: A response containing list of services.
    """
    try:
        services = wfm_db.get_all_services()
    except NoDocumentError as exc:
        status_code = 404
        msg = "No service found."
        logger.error(f"{status_code} response because {msg}")
        raise HTTPException(
            status_code=status_code,
            detail=msg
        ) from exc

    for service in services:
        # Update each service status in the DB according to the resource manager
        # command
        try:
            update_service_status_from_rm(wfm_db, service, app_settings.command)
        except UnexistingServiceNameError as exc:
            status_code = 404
            msg = f"Failed to update {service['name']} status"
            logger.error(f"{status_code} response because {msg}")
            raise HTTPException(
                status_code=status_code,
                detail=msg
            ) from exc
    return wfm_db.get_all_services()

# TODO the integration with keycloak have be added
@service_router.get("/{service_name}",
                    response_model=List[Dict[str, Any]],
                    response_model_by_alias=False,
                    response_model_exclude_unset=True,
                    summary="Get services for a given service name.")
async def get_service(
              service_name: str,
              wfm_db: WFMDatabase = Depends(wfm_database),
              app_settings: WFMSettings = Depends(WFMSettings.provider)) -> List[Dict[str, Any]]:
    """Given a service name, returns the metadata of all services with this service name.

    **Args:**\
        `service_name (str)`: The service name.

    **Returns:**\
        `List[Dict[str, Any]]`: A response containing list of services.
    """
    # First get the service info to retreive its type
    try:
        service = wfm_db.get_service_info_from_name(service_name)[0]
    except UnexistingServiceNameError as exc:
        status_code = 404
        msg = f"No service with name {service_name}"
        logger.error(f"{status_code} response because {msg}")
        raise HTTPException(
            status_code=status_code,
            detail=msg
        ) from exc

    # Then update its status in the DB according to the resource manager
    # command
    try:
        update_service_status_from_rm(wfm_db, service, app_settings.command)
    except UnexistingServiceNameError as exc:
        status_code = 404
        msg = f"Failed to update {service_name} status"
        logger.error(f"{status_code} response because {msg}")
        raise HTTPException(
            status_code=status_code,
            detail=msg
        ) from exc

    # Finally return the updated info
    try:
        return wfm_db.get_service_info_from_name(service_name)
    except UnexistingServiceNameError as exc:
        status_code = 404
        msg = f"No service with name {service_name}"
        logger.error(f"{status_code} response because {msg}")
        raise HTTPException(
            status_code=status_code,
            detail=msg
        ) from exc
