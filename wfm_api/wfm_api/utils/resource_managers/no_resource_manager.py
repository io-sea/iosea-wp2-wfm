"""
This class inherits from the ResourceManager class in order to provide ways to manipulate
resources reservation when there is no resource manager underneath.
"""
from typing import Any, Dict, List
from loguru import logger

from wfm_api.config.wfm_settings import ResourcemanagerSettings
from wfm_api.utils.resource_managers.resource_managers import ResourceManager

__copyright__ = """
Copyright (C) Bull S. A. S.
"""


class NOResourceManager(ResourceManager):
    """NO ephemeral service class
    """
    def __init__(self, resource_mgr: ResourcemanagerSettings) -> None:
        """Initialize the NO ResourceManager with appropriate values.

        Args:
            resource_mgr (ResourcemanagerSettings): settings for the resource manager

        Returns:
            None
        """
        super().__init__()

    def reserve_resources(self, request_body: Dict[str, Any]) -> int:
        """Does nothing, since there is no resource manager.

        Args:
            request_body (Dict[str, Any]): the previously built request body to be sent

        Returns:
            0 always
        """
        logger.debug("No Reservation to do")
        return 0

    def get_usable_locations(self) -> List[Dict[str, Any]]:
        """Gets all partition names that can be used

        Args:
            None

        Returns:
            List[Dict[str, Any]]: The partitions names
        """
        logger.debug("No locations to get")
        return []

    def get_usable_flavors(self) -> List[Dict[str, Any]]:
        """Gets all flavors that can be used (using the Flash Accelerators command)

        Args:
            None

        Returns:
            [] always until we are able to call sbbctrl show flavor
        """
        logger.debug("No flavors to get")
        return []
