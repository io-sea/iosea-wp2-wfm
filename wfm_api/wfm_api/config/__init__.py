"""Init file for configuration of Workflow Manager API.
"""
from pathlib import Path

__copyright__ = """
Copyright (C) 2022 Bull S. A. S. - All rights reserved
Bull, Rue Jean Jaures, B.P.68, 78340, Les Clayes-sous-Bois, France
This is not Free or Open Source software.
Please contact Bull S. A. S. for details about its license.
"""

CURRENT_DIR = Path(__file__).parent.absolute()
WFM_CONFIG = CURRENT_DIR / "default.yaml"
