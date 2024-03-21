"""Module for parsing the settings of the DASI configuration file
"""

import os
import sys
from typing import List
from pathlib import Path
import yaml
from pydantic import BaseSettings, Field
from loguru import logger

__copyright__ = """
Copyright (C) Bull S.A.S.
"""

class RootsSettings(BaseSettings):
    """Parses the spaces.roots settings.
    """
    path: str = Field(None, description="A mount path")


class SpacesSettings(BaseSettings):
    """Parses the spaces settings.
    """
    roots: List[RootsSettings] = []


class DASISettings(BaseSettings):
    """Class for parsing the settings of an DASI configuration file, using pydantic.
    """
    # The field name "schema" is reserved for a BaseModel attribute.
    # Therefore, we use a different field name "dasi_schema" with "alias='schema'".
    dasi_schema: str = Field(None, alias="schema", description="Path to schema file")
    catalogue: str = "toc"
    store: str = "file"
    spaces: List[SpacesSettings] = []

    @classmethod
    def from_yaml(cls, path: str):
        """Create a DASISettings instance from a YAML file.

        Args:
            path (str): The path to the YAML file.

        Returns:
            DASISettings: The class itself.
        """
        return cls(**yaml.load(Path(path).read_text(encoding="utf-8"),
                               Loader=yaml.SafeLoader))
