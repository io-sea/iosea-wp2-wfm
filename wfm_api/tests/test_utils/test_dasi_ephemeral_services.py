"""Unit tests for the ephemeral services common methods.
"""
import unittest
import os
from pathlib import Path

from wfm_api.config.wfm_settings import WFMSettings
from wfm_api.utils.ephemeral_services.ephemeral_services import EphemeralService
from wfm_api.utils.ephemeral_services.dasi_ephemeral_service import DASIEphemeralService

CURRENT_DIR = Path(__file__).parent.absolute()
TEST_CONFIG = f"{CURRENT_DIR}/test_data/settings.yaml"
DASI_CONFIG_FILE = "/tmp/wdf1_dasi.dasi_config.yaml"

class TestDASIEphemeralServices(unittest.TestCase):
    """Tests that DASI ephemeral services methods behave as expected.
    """

    def setUp(self):
        """Connect to database for tests.
        """
        settings = WFMSettings.from_yaml(TEST_CONFIG)
        self.dasi_services = [
            { 'name': 'srv8', 'type': 'DASI',
                'attributes':
                    { 'dasiconfig': DASI_CONFIG_FILE, 'namespace':'/tmp',
                      'storagesize': "100MiB" } },
            { 'name': 'srv9', 'type': 'DASI',
                'attributes':
                    { 'dasiconfig': 'NO_ABS_PATH', 'namespace':'/tmp', 'storagesize': "50MiB",
                      'datanodes': 5 } }
        ]
        self.ephemeral_srv = EphemeralService()
        self.ephemeral_dasi = DASIEphemeralService(settings.command)

    def test_check_service_attributes_dasi0(self):
        """Tests that check_service_attributes behaves as expected
        for a DASI ephemeral service with all the mandatory fields
        """
        attr = self.dasi_services[0]['attributes']
        result = self.ephemeral_dasi.check_service_attributes(attr)
        self.assertEqual(result, "")

    def test_check_service_attributes_dasi1(self):
        """Tests that check_service_attributes behaves as expected
        for a DASI ephemeral service when dasi config attribute is
        not an absolute pathname
        """
        error_msg = "is not an absolute pathname"
        expected_result= f"The DASI configuration file 'NO_ABS_PATH' {error_msg}"
        result = self.ephemeral_dasi.check_service_attributes(self.dasi_services[1]['attributes'])
        self.assertEqual(result, expected_result)
   
    def test_check_service_attributes_dasi2(self):
        """Tests that check_service_attributes behaves as expected
        for a DASI ephemeral service when dasi config file is unreadble
        """
        expected_result= f"Could not open DASI configuration file '{DASI_CONFIG_FILE}' for reading"
        # Make unreadable the dasi config file
        os.chmod(DASI_CONFIG_FILE, 0o200)    
        result = self.ephemeral_dasi.check_service_attributes(self.dasi_services[0]['attributes'])
        # Restore the dasi config rights
        os.chmod(DASI_CONFIG_FILE, 0o700)
        self.assertEqual(result, expected_result)

