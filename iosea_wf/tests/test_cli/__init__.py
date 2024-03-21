"""Creates a mock container that will be used for testing the CLI.
"""

import os
import unittest
import subprocess
from shutil import which
from pathlib import Path

__copyright__ = """
Copyright (C) Bull S. A. S.
"""


CURRENT_DIR = Path(__file__).parent.absolute()
TEST_CONFIG_DIR = os.path.join(CURRENT_DIR, "test_data")


class TestEntrypoints(unittest.TestCase):
    """Tests the entrypoints for creating containers work as expected."""

    @classmethod
    def setUpClass(cls):
        """Set up before all tests.
        """
        cls.config_dir = TEST_CONFIG_DIR
        cls.settings = os.path.join(TEST_CONFIG_DIR, "cli_settings.yaml")

        # Initialize the name of the workflow description file 
        # (one for SBB and one for NFS)
        # taking into account whether slurm is installed or not
        cls.wdf_sbb = os.path.join(TEST_CONFIG_DIR, "wdf1_sbb_no_slurm.yaml")
        cls.wdf_nfs = os.path.join(TEST_CONFIG_DIR, "wdf1_nfs_no_slurm.yaml")
        if which('scontrol') is not None:
            cls.wdf_sbb = os.path.join(TEST_CONFIG_DIR, "wdf1_sbb.yaml")
            cls.wdf_nfs = os.path.join(TEST_CONFIG_DIR, "wdf1_nfs.yaml")
            subprocess.run(['sudo', 'sbbctrl', 'exec', 'clear', '-y'], check=False,
                                    stdout=subprocess.PIPE)
        cls.wrong_wdf = os.path.join(TEST_CONFIG_DIR, "wdf1_wrong.yaml")

    def setUp(self) -> None:
        """Setup for before each test.
        """
