"""Models for the requests to be submitted to the service reservation metadata endpoints.
"""
from typing import Any, Dict, List
from pydantic import BaseModel, Field

__copyright__ = """
Copyright (C) Bull S.A.S.
"""


class ServiceAttributesItem(BaseModel):
    """Data model to describe the attributes of a service to be reserved.
    Used in the ServiceReservationItem.
    """
    targets: List[str] = Field(None, description="List of targets   - SBB")
    flavor: str = Field(None, description=" the flavor              - SBB")
    cores: int = Field(None, description="number of cores           - SBB (X(flavor))")
    msize: int = Field(None, description="memory size               - SBB (X(flavor))")
    ssize: int = Field(None, description="storage size              -     (X(flavor), X(gssize))")
    gssize: int = Field(None, description="global storage size      - GBF (X(ssize))")
    mountpoint: str = Field(None, description="the mount point      - GBF")


class ServiceReservationItem(BaseModel):
    """Data model to describe a service to be reserved.
    Used in the RM and in the session router to communicate between each other.
    """
    name: str = Field(None, description="The service name.")
    user: str = Field(None, description="The user login name.")
    user_slurm_token: str = Field(None, description="Token for jobs exec under the user identity.")
    srv_type: str = Field(None, description="The service type ('SBB' or 'GBF').")
    servers: int = Field(None, description="Number of datanodes.")
    attributes: ServiceAttributesItem = Field(None, description="attributes for the resources")
    location: str = Field(None, description="Partition the service should be started on.")
