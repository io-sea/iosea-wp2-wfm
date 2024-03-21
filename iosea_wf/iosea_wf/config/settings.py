"""Module for parsing the settings of the CLI.
"""
__copyright__ = """
Copyright (C) Bull S. A. S.
"""


# Disable too few public methods for pylint as inconsistent with pydantic style
# pylint: disable=too-few-public-methods

import sys
import os
from typing import Optional
from pathlib import Path
import yaml
from pydantic import BaseSettings, validator
from loguru import logger


class ServerSettings(BaseSettings):
    """Parses the settings of the server."""
    host: str = "0.0.0.0"
    port: int = 8080
    root_path: Optional[str]

    # with pre=True this validator is called before the standard validators
    @validator('port', pre=True)
    def expand_port(cls, value):
        if str(value).isdigit():
            return value
        return int(value.format(UID=os.getuid()))


class MetadataSettings(BaseSettings):
    """Parses the settings of the application.
    """
    title: str = "Default title"
    description: str = "Description of CLI"


class LoggingSettings(BaseSettings):
    """Parses the settings of the logger.
    """
    level: str = "WARNING"
    path: Optional[str] = "iosea-wf.log" # should never be used
    stderr: bool = True

    @validator('path')
    def expand_path(cls, value):
        try:
            return value.format(**os.environ)
        except Exception as except_msg:
            logger.error(f"Unable to start API, unknown {except_msg} variable in {value} path")
            sys.exit(1)

class CLISettings(BaseSettings):
    """Class for parsing the settings of an API, using pydantic.
    """
    server: ServerSettings
    metadata: MetadataSettings
    logging: LoggingSettings

    @classmethod
    def from_yaml(cls, path: str):
        """Create a CLISettings instance from a YAML file.

        Args:
            path (str): The path to the YAML file.

        Returns:
            CLISettings: The class itself.
        """
        return cls(**yaml.load(Path(path).read_text(encoding="utf-8"),
                               Loader=yaml.SafeLoader))


class WFMCLISettings(CLISettings):
    """Base settings for the WFM CLI
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        logger.remove(None)
        if self.logging.stderr:
            logger.add(sys.stderr, level=self.logging.level)
        if self.logging.path:
            logger.add(self.logging.path, level=self.logging.level)
