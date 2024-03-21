"""This module contains all errors that can be raised by the CLI.
"""

from loguru import logger

__copyright__ = """
Copyright (C) Bull S.A.S.
"""


class VariableDefinitionSyntaxError(Exception):
    """Defines an error that should be raised when there is a syntax error in the variable
    definitions provided with the --define option."""

    def __init__(self, definition_str: str = "", message: str = "") -> None:
        """Initialize the VariableDefinitionSyntaxError with an optional message.

        Args:
            definition_str (str, optional): The definition strings. Defaults to ""
            message (str, optional): The optional error message to log. Defaults to
            "Error: syntax error in variable definition.".
        """
        self.definition = definition_str

        if not message:
            self.message = f"Error: Syntax error in variable definition: \'{self.definition}\'."
        else:
            self.message = message

        logger.error(self.message)
        super().__init__(self.message)
