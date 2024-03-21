"""This module contains all errors that can be raised by the WFM API.
"""

from loguru import logger

__copyright__ = """
Copyright (C) 2022 Bull S. A. S. - All rights reserved
Bull, Rue Jean Jaures, B.P.68, 78340, Les Clayes-sous-Bois, France
This is not Free or Open Source software.
Please contact Bull S. A. S. for details about its license.
"""


class UnexistingServiceNameError(Exception):
    """Defines an error that should be raised when there is no service entry available for a
    service name."""

    def __init__(self, servicename: str = "", message: str = "") -> None:
        """Initialize the UnexistingServiceNameError with an optional message.

        Args:
            servicename (str, optional): The Service name. Defaults to ""
            message (str, optional): The optional error message to log. Defaults to
            "Error: Service not found.".
        """
        self.servicename = servicename

        if not message:
            self.message = f"Error: Service {self.servicename} not found."
        else:
            self.message = message

        logger.error(self.message)
        super().__init__(message)


class UnexistingSessionNameError(Exception):
    """Defines an error that should be raised when there is no session entry available for a
    session name."""

    def __init__(self,
                 sessionname: str = "",
                 workflowname: str = "",
                 message: str = "") -> None:
        """Initialize the UnexistingSessionNameError with an optional message.

        Args:
            sessionname (str, optional): The Service name. Defaults to ""
            workflowname (str, optional): The Workflow name. Defaults to ""
            message (str, optional): The optional error message to log. Defaults to
            "Error: Session not found.".
        """
        self.sessionname = sessionname
        self.workflowname = workflowname

        if not message:
            if not workflowname:
                self.message = f"Error: Session {self.sessionname} not found."
            else:
                self.message = (f"Error: Session {self.sessionname} not found "
                                f"for workflow {self.workflowname}.")
        else:
            self.message = message

        logger.error(self.message)
        super().__init__(message)


class NoDocumentError(Exception):
    """Defines an error to raise when there is no entry matching a given request."""

    def __init__(self, message: str = "") -> None:
        """Initialize the NoDocumentError with an optional ObjectId and an optional message.

        Args:
            message (str, optional): The optional error message to log. Defaults to "".
            "Error: Entry not found."
        """
        if not message:
            self.message = "Error: Entry not found."
        else:
            self.message = message

        logger.error(self.message)
        super().__init__(message)

class NoUniqueDocumentError(Exception):
    """Defines an error to raise when there is more than one
    entry matching a given request."""

    def __init__(self, message: str = "") -> None:
        """Initialize the NoUniqueDocumentError with an optional message.

        Args:
            message (str, optional): The optional error message to log. Defaults to "".
            "Error: Multiple matches were found."
        """
        if not message:
            self.message = "Error: Multiple matches were found."
        else:
            self.message = message

        logger.error(self.message)
        super().__init__(message)
