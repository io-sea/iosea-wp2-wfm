"""Model for the WFM Step metadata endpoint.
"""
from typing import Any, Dict, List
from pydantic import BaseModel, Field

__copyright__ = """
Copyright (C) 2022-2023 Bull S. A. S. - All rights reserved
Bull, Rue Jean Jaures, B.P.68, 78340, Les Clayes-sous-Bois, France
This is not Free or Open Source software.
Please contact Bull S. A. S. for details about its license.
"""


class StepStartItem(BaseModel):
    """Data model to describe a step to be started.
    Used in the step router.
    """
    session_name: str = Field(None, description="Name of the session the step belongs to")
    step_name: str = Field(None, description="Name of the step to be started")
    replacements: Dict[str, str] = Field(None, description="Variables replacement values")


class StepDescriptionItem(BaseModel):
    """Data model to describe a step.
    """
    id: int = Field(None, description="Unique step ID")
    session_id: int = Field(None, description="Session id")
    name: str = Field(None, description="Step name")
    command: str = Field(None, description="Step command")
    service_id: int = Field(None, description="Step service")
    steps: List[Dict[str, Any]] = Field(None, description="Steps for this step description")


class StepProgressItem(BaseModel):
    """Data model to describe the progress of a step instance.
    Used in the step router.
    """
    jobid: int = Field(None, description="JobID related to the step whose progress is updated.")
    progress: str = Field(None, description="Step progress")


class StepItem(BaseModel):
    """Data model to describe a step instance.
    """
    id: int = Field(None, description="Unique step instance ID")
    step_description_id: int = Field(None, description="Session description id.")
    instance_name: str = Field(None, description="Step instance name")
    start_time: int = Field(None, description="Timestamp of the step execution beginning.")
    stop_time: int = Field(None, description="Timestamp of the step execution end.")
    # TODO: declare it as an Enum when the set of allowed status is fixed
    status: str = Field(None, description="Step status: starting / running / stopped / failed.")
    progress: str = Field(None, description="Step progress")
    command: str = Field(None, description="Step command with variables instantiated")
    jobid: int = Field(None, description="JobID of the job that is running the step.")
