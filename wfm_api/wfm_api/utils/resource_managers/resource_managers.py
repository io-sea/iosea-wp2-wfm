"""
The goal of the ResourceManager class is to provide an abstraction of the resource
managers. It provides the methods that must be implemented when adding support
for a new Resource Manager.
"""
from abc import abstractmethod
from typing import Any, Dict, List
from loguru import logger

__copyright__ = """
Copyright (C) Bull S. A. S.
"""


class ResourceManager:
    """
    Represents a resource manager (defined as an abstract interface).

    This class provides methods to:
        - Reserve resources for a given service (reserve_resources).
        - Get all locations available to the user (get_usable_locations)
        - Get all flavors available to the user (get_usable_flavors)
    """
    def __init__(self):
        """Initializes the instance variables
        """

    @abstractmethod
    def reserve_resources(self, request_body: Dict[str, Any]) -> int:
        """Reserves a set of resources for an ephemeral service.

        Args:
            request_body (Dict[str, Any]): the previously built request body to be sent

        Returns:
            0 on success
            -1 on failure
        """

    @abstractmethod
    def get_usable_locations(self) -> List[Dict[str, Any]]:
        """Gets all partition names that can be used

        Args:
            None

        Returns:
            List[Dict[str, Any]]: The partitions names
        """

    @abstractmethod
    def get_usable_flavors(self) -> List[Dict[str, Any]]:
        """Gets all flavors that can be used with their resources characteristics

        Args:
            None

        Returns:
            List[Dict[str, Any]]: The flavors descriptions
        """
