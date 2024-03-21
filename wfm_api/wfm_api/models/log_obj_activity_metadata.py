"""Model for the WFM object activity logging metadata endpoint.
"""
from pydantic import BaseModel, Field

__copyright__ = """
Copyright (C) 2023 Bull S. A. S. - All rights reserved
Bull, Rue Jean Jaures, B.P.68, 78340, Les Clayes-sous-Bois, France
This is not Free or Open Source software.
Please contact Bull S. A. S. for details about its license.
"""


class ObjectActivityLoggingItem(BaseModel):
    """Data model to describe an object activity logging.
    An item is created upon object creation.
    Creation and deletion are logged here.
    """
    id: int = Field(None, description="Unique Object Activity Logging ID.")
    # TODO: declare it as an Enum when the set of allowed types is fixed
    object_type: str = Field(None,
                             description="Object type: session/service/step_description/step.")
    object_id: int = Field(None,
                           description="Object id: session/service/step_description/step ids.")
    # TODO: declare it as an Enum when the set of covered activities is fixed
    activity: str = Field(None, description="Object activity: creation / removal.")
    time: int = Field(None, description="Activity timestamp.")
