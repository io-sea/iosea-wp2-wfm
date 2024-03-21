"""Models for the WFM Session metadata endpoints.
"""
from typing import Any, Dict, List
from pydantic import BaseModel, Field

__copyright__ = """
Copyright (C) Bull S.A.S.
"""


class SessionStartItem(BaseModel):
    """Data model to describe a session to be started.
    Used in the session router.
    """
    workflow_description_file: str = Field(None, description="Path to the WDF")
    workflow_description: str = Field(None, description="The WDF as yaml str")
    sync_start: bool = Field(None, description="Whether to synchronously start the services")
    session_name: str = Field(None, description="Name of the session to be started")
    user_name: str = Field(None, description="Login of the user that called the CLI")
    replacements: Dict[str, str] = Field(None, description="Variables replacement values")


class SessionStopItem(BaseModel):
    """Data model to describe a session to be stopped.
    Used in the session router.
    """
    sync_stop: bool = Field(None, description="Whether to synchronously stop the services")
    session_name: str = Field(None, description="Name of the session to be stopped")


class SessionAccessItem(BaseModel):
    """Data model to describe a session to be accessed.
    Used in the session router.
    """
    session_name: str = Field(None, description="Name of the session to be accessed")
    services: List[str] = Field(None, description="Session services to be used")


class SessionItem(BaseModel):
    """Data model to describe a session.
    """
    id: str = Field(None, description="Unique session ID")
    name: str = Field(None, description="Session name")
    workflow_name: str = Field(None, description="Owning Workflow name")
    start_time: int = Field(None, description="Timestamp of the session beginning")
    end_time: int = Field(None, description="Timestamp when the session ended")
    # TODO: declare it as an Enum when the set of allowed status is fixed
    status: str = Field(None, description="starting/active/stopping/stopped/teardown")
    services: List[Dict[str, Any]] = Field(None, description="Session services")
    step_descriptions: List[Dict[str, Any]] = Field(None, description="Session step descriptions")
 #   startTime: Timestamp = Field(
 #       None, description="Timestamp of the session beginning")
 #   endTime: Timestamp = Field(
 #       None, description="Timestamp when the session ended")
