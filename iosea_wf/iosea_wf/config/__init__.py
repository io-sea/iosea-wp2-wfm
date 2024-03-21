"""Init file for configuration of Workflow Manager CLI.
"""
import os
from pathlib import Path

__copyright__ = """
Copyright (C) Bull S. A. S.
"""

CURRENT_DIR = Path(__file__).parent.absolute()
home_directory = Path.home()
local_settings = os.path.join(home_directory, '.iosea_wf_settings.yml')
if os.path.isfile(local_settings):
    WFM_CLI_CONFIG = local_settings
else:
    WFM_CLI_CONFIG = os.path.join(CURRENT_DIR, 'default.yaml')
