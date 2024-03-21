"""Contains endpoints that show some configuration elements
"""

from typing import Any, Dict, List
from fastapi import APIRouter, Depends

from pax.providers.oidc.provider import check_user
from pax.providers.oidc.models import UserClaims
from wfm_api.config.wfm_settings import WFMSettings
from wfm_api.utils.utils import get_usable_locations, get_usable_flavors

__copyright__ = """
Copyright (C) Bull S. A. S.
"""

configuration_router = APIRouter(prefix="/configuration", tags=["Configuration Data"])

@configuration_router.get("/locations",
                          response_model=List[Dict[str, Any]],
                          response_model_by_alias=False,
                          response_model_exclude_unset=True,
                          summary="Get all locations.")
async def get_all_locations(
              app_settings: WFMSettings = Depends(WFMSettings.provider)) -> List[Dict[str, Any]]:
    """Returns all locations

    **Args:**\
        `app_settings (WFMSettings)`: The configuration settings.

    **Returns:**\
        `List[Dict[str, Any]]`: A response containing the list of locations.
    """
    return get_usable_locations(app_settings.jobmanager.name,
                                app_settings.command,
                                app_settings.resourcemanager)


@configuration_router.get("/flavors",
                          response_model=List[Dict[str, Any]],
                          response_model_by_alias=False,
                          response_model_exclude_unset=True,
                          summary="Get all flavors.")
async def get_all_flavors(
              app_settings: WFMSettings = Depends(WFMSettings.provider)) -> List[Dict[str, Any]]:
    """Returns all flavors available for the service in parameter

    **Args:**\
        `app_settings (WFMSettings)`: The configuration settings.

    **Returns:**\
        `List[Dict[str, Any]]`: A response containing the list of flavors.
    """
    return get_usable_flavors(app_settings.resourcemanager)
