"""Create the WFM API through an app container.
"""

import os
from typing import Callable, List
import click
from fastapi import APIRouter
from pax.entrypoint import create_container, pax_main
from wfm_api.config.wfm_settings import WFMSettings
from wfm_api.config import WFM_CONFIG
from wfm_api.pax_hooks.wfm_database import wfm_database_hook
from wfm_api.routers import wfm_routers

__copyright__ = """
Copyright (C) 2022 Bull S. A. S. - All rights reserved
Bull, Rue Jean Jaures, B.P.68, 78340, Les Clayes-sous-Bois, France
This is not Free or Open Source software.
Please contact Bull S. A. S. for details about its license.
"""


@click.command()
@click.option('--settings', default=WFM_CONFIG, help='Path to the configuration file.')
def main(settings: str,
         factory: str = "wfm_api.app:wfm_container_factory"):
    """Main function running the Python API"""
    os.environ["PY_API_SETTINGS"] = settings
    pax_main(app_settings=WFMSettings.from_yaml(settings),
             factory=factory)


def wfm_container_factory(
    routers: List[APIRouter] = wfm_routers,
    hooks: List[Callable] = [wfm_database_hook]
):
    """Function to create a container for the WFM API.

    Args:
        routers (List[APIRouter], optional): The list of routers to attach to the application.
        Defaults to wfm_routers.
        hooks (List[Callable]): The list of hooks to include in the application.

    Returns:
        A WFM API application
    """
   # Parse input settings
    app_settings = WFMSettings.from_yaml(os.environ["PY_API_SETTINGS"])
    container = create_container(
        settings=app_settings,
        routers=routers,
        hooks=hooks)
    return container.app
