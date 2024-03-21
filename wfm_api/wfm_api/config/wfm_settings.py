"""Configuration file specific to the WFM database, inherited from PAX standard one.
"""

import os
import sys
from typing import Optional
from pydantic import BaseSettings, validator
from loguru import logger
from pax.settings.settings import AppSettings, ServerSettings

__copyright__ = """
Copyright (C) Bull S.A.S.
"""


class CommandSettings(BaseSettings):
    """Parses the command settings.
    """
    job_submission_cmd: str = "/usr/bin/srun"
    job_control_cmd: str = "/usr/bin/scontrol"
    job_batch_cmd: str = "/usr/bin/sbatch"
    job_cancel_cmd: str = "/usr/bin/scancel"
    job_state_cmd: str = "/usr/bin/squeue"


class JobmanagerSettings(BaseSettings):
    """Parses the job manager settings.
    """
    name: str = "SLURM"


class ResourcemanagerSettings(BaseSettings):
    """Parses the resource manager settings.
    """
    name: str = "NONE"
    version: str = "v2.0.0"
    host: str = "0.0.0.0"
    port: int = 8080
    root_path: Optional[str] = "/"


class DatabaseSettings(BaseSettings):
    """Parses the database settings.
    """
    enabled: bool = False
    name: str = ":memory:"

    @validator('name')
    def expand_name(cls, value):
        try:
            return value.format(**os.environ)
        except Exception as except_msg:
            print(f"Unable to start API, unknown {except_msg} variable in {value} database name")
            sys.exit(1)

class LoggingSettings(BaseSettings):
    """Parses the settings of the logger.
    """
    level: str = "INFO"
    path: Optional[str] = "wfm-api.log"  # should never be used
    stderr: bool = False

    @validator('path')
    def expand_path(cls, value):
        try:
            return value.format(**os.environ)
        except Exception as except_msg:
            print(f"Unable to start API, unknown {except_msg} variable in {value} path")
            sys.exit(1)


class WFMServerSettings(ServerSettings):
    """Parses the settings of the server."""
    port: int = 8080

    # with pre=True this validator is called before the standard validators
    @validator('port', pre=True)
    def expand_port(cls, value):
        if str(value).isdigit():
            return value
        return int(value.format(UID=os.getuid()))


class WFMSettings(AppSettings):
    """Base settings for the WFM application.
    """
    command: CommandSettings = CommandSettings()
    jobmanager: JobmanagerSettings = JobmanagerSettings()
    resourcemanager: ResourcemanagerSettings = ResourcemanagerSettings()
    database: DatabaseSettings = DatabaseSettings()
    logging: LoggingSettings
    server: WFMServerSettings

    def __init__(self, **kwargs):
        logger.remove(None)
        super().__init__(**kwargs)
