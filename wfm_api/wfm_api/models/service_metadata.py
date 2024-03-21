"""Model for the WFM Service metadata endpoint.
"""
from typing import Optional, Any, Dict, List
from pydantic import BaseModel, Field

__copyright__ = """
Copyright (C) Bull S.A.S.
"""


class ServiceItem(BaseModel):
    """Data model to describe a service.
    """
    id: int = Field(None, description="Unique service ID")
    session_id: int = Field(None, description="Session id")
    step_descriptions: List[Dict[str, Any]] = Field(None, description="Step descriptions that use this service")
    name: str = Field(None, description="Service name")
    # TODO: declare it as an Enum when the set of allowed services is fixed
    service_type: str = Field(None, description="Service type")
    location: str = Field(None, description="Service location")
    # SBB only
    targets: str = Field(None, description="The service targets.")
    flavor: str = Field(None, description="The service flavor.")
    # GBF only
    namespace: str = Field(None, description="The service namespace.")
    mountpoint: str = Field(None, description="The service mountpoint.")
    storagesize: str = Field(None, description="The service storagesize.")
    datanodes: Optional[int] = Field(None, description="The number of datanodes the service will be running on.")
    # TODO: declare it as an Enum when the set of allowed status is fixed
    start_time: int = Field(None, description="The service start time") # start session timestamp
    end_time: int = Field(None, description="The service start time") # end session timestamp
    status: str = Field(None, description="Service status: see ServiceStatus in wfm database.")


class NamespaceLockItem(BaseModel):
    """Data model to describe a namespace.
    """
    id: int = Field(None, description="Unique namespace ID")
    ns_name: str = Field(None, description="Nmespace name")
    service_name: str = Field(None, description="Owning service name")
