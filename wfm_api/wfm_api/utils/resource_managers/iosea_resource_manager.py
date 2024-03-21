"""
This class inherits from the ResourceManager class in order to provide ways to manipulate
resources with the resource manager developed by IT4I.
"""
from typing import Any, Dict, List
from loguru import logger
import httpx

from wfm_api.config.wfm_settings import ResourcemanagerSettings
from wfm_api.utils.resource_managers.resource_managers import ResourceManager

__copyright__ = """
Copyright (C) Bull S. A. S.
"""


class IOSEAResourceManager(ResourceManager):
    """IOSEA resource manager class
    """
    def __init__(self, resource_mgr: ResourcemanagerSettings) -> None:
        """Initialize the IOSEA Resource Manager with appropriate values.

        Args:
            resource_mgr (ResourcemanagerSettings): settings for the resource manager

        Returns:
            None
        """
        super().__init__()
        self.host = resource_mgr.host
        self.port = resource_mgr.port
        self.root_path = resource_mgr.root_path
        self.api_base_url = (f"http://{resource_mgr.host}:{resource_mgr.port}"
                             f"{resource_mgr.root_path}")
        self.reserve_endpoint = f"{resource_mgr.version}/ephemeralservice/reserve"
        self.reserve_url = f"{self.api_base_url}{self.reserve_endpoint}"
        self.location_endpoint = f"{resource_mgr.version}/location/list"
        self.location_url = f"{self.api_base_url}{self.location_endpoint}"
        self.flavors_endpoint = f"{resource_mgr.version}/ephemeralservice/flavors"
        self.flavors_url = f"{self.api_base_url}{self.flavors_endpoint}"

    def reserve_resources(self, request_body: Dict[str, Any]) -> int:
        """Reserves a set of resources for an ephemeral service.

        Args:
            request_body (Dict[str, Any]): the previously built request body to be sent
                                           should be coherent with ServiceReservationItem
                                           defined in rm_api/rm_api/models/resa_metadat.py

        Returns:
            int: 0 on success
                 -1 on failure
        """
        with httpx.Client(verify=False, timeout=60) as client:
            try:
                logger.debug(f"POST({self.reserve_url}, json={request_body})")
                response = client.post(self.reserve_url, json=request_body)
                logger.debug(f"GOT RESPONSE {response.json()}")
                if response.status_code == httpx.codes.OK:
                    logger.info(f"RESERVATION SUCCESSFUL FOR REQUEST {request_body}")
                    return 0
                else:
                    logger.error(f"RESERVATION FAILED FOR REQUEST {request_body}")
                    logger.error(f"GOT ERROR {response.status_code} : {response.json()['message']}")
                    return -1

            except httpx.ConnectError as exception:
                logger.error(f"Cannot connect to the RM API ({self.reserve_url})")
            else:
                logger.error(f"GOT EXCEPTION: {exception}")
            return -1

    def get_usable_locations(self) -> List[Dict[str, Any]]:
        """Gets all partition names that can be used

        Args:
            None

        Returns:
            List[Dict[str, Any]]: The partitions names
        """
        with httpx.Client(verify=False, timeout=60) as client:
            try:
                logger.debug(f"GET({self.location_url})")
                response = client.get(self.location_url)
                logger.debug(f"GOT RESPONSE {response.json()}")
                if response.status_code == httpx.codes.OK:
                    logger.debug(f"GOT PARTITIONS: {response.json()}")
                    return response.json()
                else:
                    logger.error(f"GOT ERROR {response.status_code} : {response.json()['message']}")
                    return []

            except httpx.ConnectError:
                logger.error(f"Cannot connect to the RM API ({self.location_url})")
            else:
                logger.error(response.json()['message'])
            return []

    def get_usable_flavors(self) -> List[Dict[str, Any]]:
        """Gets all flavors that can be used with their descriptions

        Args:
            None

        Returns:
            List[Dict[str, Any]]: The flavors descriptions
        """
        with httpx.Client(verify=False, timeout=60) as client:
            try:
                logger.debug(f"GET({self.flavors_url})")
                response = client.get(self.flavors_url)
                logger.debug(f"GOT RESPONSE {response.json()}")
                if response.status_code == httpx.codes.OK:
                    logger.debug(f"GOT FLAVORS: {response.json()}")
                    return response.json()
                else:
                    logger.error(f"GOT ERROR {response.status_code} : {response.json()['detail']}")
                    return []

            except httpx.ConnectError:
                logger.error(f"Cannot connect to the RM API ({self.location_url})")
            else:
                logger.error(response.json()['message'])
            return []
