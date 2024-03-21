"""Unit tests for the ephemeral services common methods.
"""
import os
import unittest
from pathlib import Path
import tempfile
import time
from shutil import rmtree

from wfm_api.config.wfm_settings import WFMSettings
from wfm_api.utils.ephemeral_services.ephemeral_services import EphemeralService
from wfm_api.utils.ephemeral_services.sbb_ephemeral_service import SBBEphemeralService
from wfm_api.utils.ephemeral_services.gbf_ganesha_ephemeral_service import GBFGaneshaEphemeralService

CURRENT_DIR = Path(__file__).parent.absolute()
TEST_CONFIG = CURRENT_DIR / "test_data" / "settings.yaml"

class TestFillReservationRequest(unittest.TestCase):
    """Tests that ephemeral services methods behave as expected.
    """

    def setUp(self):
        """Connect to database for tests.
        """
        settings = WFMSettings.from_yaml(TEST_CONFIG)
        self.sbb_services = [
            { 'name': 'srv0', 'type': 'SBB',
              'attributes': { 'targets': '/target0', 'flavor': 'flavor0' } },
            { 'name': 'srv1', 'type': 'SBB',
              'attributes': { 'targets': '/target1', 'flavor': 'flavor1',
                              'datanodes': 2 } },
            { 'name': 'srv2', 'type': 'SBB',
              'attributes': { 'targets': '/target2', 'flavor': 'flavor2',
                              'location': 'p21,p22' } },
            { 'name': 'srv3', 'type': 'SBB',
              'attributes': { 'targets': '/target3', 'flavor': 'flavor3',
                              'datanodes': 3, 'location': 'p31,p32' } }
        ]
        self.gbf_services = [
            { 'name': 'srv4', 'type': 'GBF',
                'attributes':
                    { 'targets': '/target4', 'storagesize': "400MiB", 'mountpoint': 'mnt4' } },
            { 'name': 'srv5', 'type': 'GBF',
                'attributes':
                    { 'targets': '/target5', 'storagesize': "500MiB", 'mountpoint': 'mnt5',
                      'datanodes': 5 } },
            { 'name': 'srv6', 'type': 'GBF',
                'attributes':
                    { 'targets': '/target6', 'storagesize': "600MiB", 'mountpoint': 'mnt6',
                      'location': 'p61,p62' } },
            { 'name': 'srv7', 'type': 'GBF',
                'attributes':
                    { 'targets': '/target7', 'storagesize': "700MiB", 'mountpoint': 'mnt7',
                      'datanodes': 7, 'location': 'p71,p72' } }
        ]
        self.ephemeral_srv = EphemeralService()
        self.ephemeral_sbb = SBBEphemeralService(settings.command)
        self.ephemeral_gbf = GBFGaneshaEphemeralService(settings.command)
        self.user = 'user0'
        self.user_slurm_token = 'MYTOKEN'
        self.initial_servers = 1
        self.initial_location = []

    def test_fill_reservation_request_sbb0(self):
        """Tests that fill_reservation_request behaves as expected
        for an SBB ephemeral service with only the mandatory fields
        """
        result = self.ephemeral_sbb.fill_reservation_request(self.sbb_services[0],
                                                             self.user)
        expected_result = {
            'name': self.sbb_services[0]['name'],
            'user': self.user,
            'user_slurm_token': self.user_slurm_token,
            'type': 'SBB',
            'servers' : self.initial_servers,
            'location': self.initial_location,
            'attributes': {
                'flavor': self.sbb_services[0]['attributes']['flavor'],
                'targets': self.sbb_services[0]['attributes']['targets'].split(':')
            }
        }
        self.assertDictEqual(result, expected_result)

    def test_fill_reservation_request_sbb1(self):
        """Tests that fill_reservation_request behaves as expected
        for an SBB ephemeral service with the mandatory fields + datanodes
        """
        result = self.ephemeral_sbb.fill_reservation_request(self.sbb_services[1],
                                                             self.user)
        expected_result = {
            'name': self.sbb_services[1]['name'],
            'user': self.user,
            'user_slurm_token': self.user_slurm_token,
            'type': 'SBB',
            'servers' : self.sbb_services[1]['attributes']['datanodes'],
            'location': self.initial_location,
            'attributes': {
                'flavor': self.sbb_services[1]['attributes']['flavor'],
                'targets': self.sbb_services[1]['attributes']['targets'].split(':')
            }
        }
        self.assertDictEqual(result, expected_result)

    def test_fill_reservation_request_sbb2(self):
        """Tests that fill_reservation_request behaves as expected
        for an SBB ephemeral service with the mandatory fields + location
        """
        result = self.ephemeral_sbb.fill_reservation_request(self.sbb_services[2],
                                                             self.user)
        expected_result = {
            'name': self.sbb_services[2]['name'],
            'user': self.user,
            'user_slurm_token': self.user_slurm_token,
            'type': 'SBB',
            'servers' : self.initial_servers,
            'location' : self.sbb_services[2]['attributes']['location'].split(','),
            'attributes': {
                'flavor': self.sbb_services[2]['attributes']['flavor'],
                'targets': self.sbb_services[2]['attributes']['targets'].split(':')
            }
        }
        self.assertDictEqual(result, expected_result)

    def test_fill_reservation_request_sbb3(self):
        """Tests that fill_reservation_request behaves as expected
        for an SBB ephemeral service with the mandatory fields + datanodes
        + location
        """
        result = self.ephemeral_sbb.fill_reservation_request(self.sbb_services[3],
                                                             self.user)
        expected_result = {
            'name': self.sbb_services[3]['name'],
            'user': self.user,
            'user_slurm_token': self.user_slurm_token,
            'type': 'SBB',
            'servers' : self.sbb_services[3]['attributes']['datanodes'],
            'location' : self.sbb_services[3]['attributes']['location'].split(','),
            'attributes': {
                'flavor': self.sbb_services[3]['attributes']['flavor'],
                'targets': self.sbb_services[3]['attributes']['targets'].split(':')
            }
        }
        self.assertDictEqual(result, expected_result)

    def test_fill_reservation_request_gbf0(self):
        """Tests that fill_reservation_request behaves as expected
        for a GBF ephemeral service with only the mandatory fields
        """
        result = self.ephemeral_gbf.fill_reservation_request(self.gbf_services[0],
                                                             self.user)
        expected_result = {
            'name': self.gbf_services[0]['name'],
            'user': self.user,
            'user_slurm_token': self.user_slurm_token,
            'type': 'GBF',
            'servers' : self.initial_servers,
            'location': self.initial_location,
            'attributes': {
                'gssize': self.gbf_services[0]['attributes']['storagesize'],
                'mountpoint': self.gbf_services[0]['attributes']['mountpoint']
            }
        }
        self.assertDictEqual(result, expected_result)

    def test_fill_reservation_request_gbf1(self):
        """Tests that fill_reservation_request behaves as expected
        for a GBF ephemeral service with the mandatory fields + datanodes
        """
        result = self.ephemeral_gbf.fill_reservation_request(self.gbf_services[1],
                                                             self.user)
        expected_result = {
            'name': self.gbf_services[1]['name'],
            'user': self.user,
            'user_slurm_token': self.user_slurm_token,
            'type': 'GBF',
            'servers' : self.gbf_services[1]['attributes']['datanodes'],
            'location': self.initial_location,
            'attributes': {
                'gssize': self.gbf_services[1]['attributes']['storagesize'],
                'mountpoint': self.gbf_services[1]['attributes']['mountpoint']
            }
        }
        self.assertDictEqual(result, expected_result)

    def test_fill_reservation_request_gbf2(self):
        """Tests that fill_reservation_request behaves as expected
        for a GBF ephemeral service with the mandatory fields + location
        """
        result = self.ephemeral_gbf.fill_reservation_request(self.gbf_services[2],
                                                             self.user)
        expected_result = {
            'name': self.gbf_services[2]['name'],
            'user': self.user,
            'user_slurm_token': self.user_slurm_token,
            'type': 'GBF',
            'servers' : self.initial_servers,
            'location' : self.gbf_services[2]['attributes']['location'].split(','),
            'attributes': {
                'gssize': self.gbf_services[2]['attributes']['storagesize'],
                'mountpoint': self.gbf_services[2]['attributes']['mountpoint']
            }
        }
        self.assertDictEqual(result, expected_result)

    def test_fill_reservation_request_gbf3(self):
        """Tests that fill_reservation_request behaves as expected
        for a GBF ephemeral service with the mandatory fields + datanodes
        + location
        """
        result = self.ephemeral_gbf.fill_reservation_request(self.gbf_services[3],
                                                             self.user)
        expected_result = {
            'name': self.gbf_services[3]['name'],
            'user': self.user,
            'user_slurm_token': self.user_slurm_token,
            'type': 'GBF',
            'servers' : self.gbf_services[3]['attributes']['datanodes'],
            'location' : self.gbf_services[3]['attributes']['location'].split(','),
            'attributes': {
                'gssize': self.gbf_services[3]['attributes']['storagesize'],
                'mountpoint': self.gbf_services[3]['attributes']['mountpoint']
            }
        }
        self.assertDictEqual(result, expected_result)


class TestIsValidHestiaSourceFile(unittest.TestCase):
    """Tests that is_valid_hestia_source_file behaves as expected.
    """

    def setUp(self):
        """Connect to database for tests.
        """
        settings = WFMSettings.from_yaml(TEST_CONFIG)
        self.ephemeral_gbf = GBFGaneshaEphemeralService(settings.command)

    def test_is_valid_hestia_source_file_empty_file(self):
        """Tests that is_valid_hestia_source_file behaves as expected
        when input file is empty
        """
        # Create an empty file
        temporary = tempfile.mkstemp()
        result = self.ephemeral_gbf.is_valid_hestia_source_file(temporary[1])
        os.remove(temporary[1])
        self.assertEqual(result, False)

    def test_is_valid_hestia_source_file_unreadable_file(self):
        """Tests that is_valid_hestia_source_file behaves as expected
        when input file is not readable
        """
        # Create a file with 2 lines, valid format
        temporary = tempfile.mkstemp()
        fname = temporary[1]
        with open(fname, 'a') as test_file:
            test_file.write('HDR_OBJECTID\n')
            test_file.write('DATA_OBJECTID\n')
        # Make it unreadable
        os.chmod(fname, 0o200)
        result = self.ephemeral_gbf.is_valid_hestia_source_file(fname)
        os.chmod(fname, 0o700)
        os.remove(fname)
        self.assertEqual(result, False)

    def test_is_valid_hestia_source_file_invalid_file0(self):
        """Tests that is_valid_hestia_source_file behaves as expected
        when input file contains 1 line
        """
        # Create a file with 1 line
        temporary = tempfile.mkstemp()
        fname = temporary[1]
        with open(fname, 'a') as test_file:
            test_file.write('HDR_OBJECTID\n')
        result = self.ephemeral_gbf.is_valid_hestia_source_file(fname)
        os.remove(fname)
        self.assertEqual(result, False)

    def test_is_valid_hestia_source_file_invalid_file1(self):
        """Tests that is_valid_hestia_source_file behaves as expected
        when input file contains 3 lines
        """
        # Create a file with 3 lines
        temporary = tempfile.mkstemp()
        fname = temporary[1]
        with open(fname, 'a') as test_file:
            test_file.write('HDR_OBJECTID\n')
            test_file.write('DATA_OBJECTID\n')
            test_file.write('ANYTHING\n')
        result = self.ephemeral_gbf.is_valid_hestia_source_file(fname)
        os.remove(fname)
        self.assertEqual(result, False)

    def test_is_valid_hestia_source_file_invalid_file2(self):
        """Tests that is_valid_hestia_source_file behaves as expected
        when input file contains 2 lines, but with incorrect format
        on 1st line
        """
        # Create a file with 2 lines, 1st = incorrect format
        temporary = tempfile.mkstemp()
        fname = temporary[1]
        with open(fname, 'a') as test_file:
            test_file.write('HDR_OBJECTID0 HDR_OBJECTID1\n')
            test_file.write('DATA_OBJECTID\n')
        result = self.ephemeral_gbf.is_valid_hestia_source_file(fname)
        os.remove(fname)
        self.assertEqual(result, False)

    def test_is_valid_hestia_source_file_invalid_file3(self):
        """Tests that is_valid_hestia_source_file behaves as expected
        when input file contains 2 lines, but with incorrect format
        on 2nd line
        """
        # Create a file with 2 lines, 2nd = incorrect format
        temporary = tempfile.mkstemp()
        fname = temporary[1]
        with open(fname, 'a') as test_file:
            test_file.write('HDR_OBJECTID\n')
            test_file.write('DATA_OBJECTID0 DATA_OBJECTID1\n')
        result = self.ephemeral_gbf.is_valid_hestia_source_file(fname)
        os.remove(fname)
        self.assertEqual(result, False)

    def test_is_valid_hestia_source_file_valid_file(self):
        """Tests that is_valid_hestia_source_file behaves as expected
        when input file has a valid format
        """
        # Create a file with 2 lines, valid format
        temporary = tempfile.mkstemp()
        fname = temporary[1]
        with open(fname, 'a') as test_file:
            test_file.write('HDR_OBJECTID\n')
            test_file.write('DATA_OBJECTID\n')
        result = self.ephemeral_gbf.is_valid_hestia_source_file(fname)
        os.remove(fname)
        self.assertEqual(result, True)


class TestIsValidHestiaSourceDir(unittest.TestCase):
    """Tests that is_valid_hestia_source_dir behaves as expected.
    """

    def setUp(self):
        """Connect to database for tests.
        """
        settings = WFMSettings.from_yaml(TEST_CONFIG)
        self.ephemeral_gbf = GBFGaneshaEphemeralService(settings.command)

    def test_is_valid_hestia_source_dir_empty_dir(self):
        """Tests that is_valid_hestia_source_dir behaves as expected
        when input directory is empty
        """
        # Create an empty directory
        tempdir = tempfile.mkdtemp()
        result, msg = self.ephemeral_gbf.is_valid_hestia_source_dir(tempdir)
        os.rmdir(tempdir)
        self.assertEqual(result, False)

    def test_is_valid_hestia_source_dir_unreadable_dir(self):
        """Tests that is_valid_hestia_source_dir behaves as expected
        when input directory is not readable
        """
        # Create a directory
        tempdir = tempfile.mkdtemp()
        # Create there a file with 2 lines, valid format
        temporary = tempfile.mkstemp(dir=tempdir)
        fname = temporary[1]
        with open(fname, 'a') as test_file:
            test_file.write('HDR_OBJECTID\n')
            test_file.write('DATA_OBJECTID\n')
        # Make the directory unreadable
        os.chmod(tempdir, 0o200)
        result, msg = self.ephemeral_gbf.is_valid_hestia_source_dir(tempdir)
        os.chmod(tempdir, 0o700)
        rmtree(tempdir, ignore_errors=True)
        self.assertEqual(result, False)

    def test_is_valid_hestia_source_dir_invalid_dir0(self):
        """Tests that is_valid_hestia_source_dir behaves as expected
        when input directory contains 1 file, invalid format
        """
        # Create a file with 1 line
        tempdir = tempfile.mkdtemp()
        temporary = tempfile.mkstemp(dir=tempdir)
        fname = temporary[1]
        with open(fname, 'a') as test_file:
            test_file.write('HDR_OBJECTID\n')
        result, msg = self.ephemeral_gbf.is_valid_hestia_source_dir(tempdir)
        rmtree(tempdir, ignore_errors=True)
        self.assertEqual(result, False)

    def test_is_valid_hestia_source_dir_invalid_dir1(self):
        """Tests that is_valid_hestia_source_dir behaves as expected
        when input dir contains 2 files and newest one has an invalid format
        """
        tempdir = tempfile.mkdtemp()
        temporary = tempfile.mkstemp(dir=tempdir)
        # Oldest file with valid format
        fname = temporary[1]
        with open(fname, 'a') as test_file:
            test_file.write('HDR_OBJECTID\n')
            test_file.write('DATA_OBJECTID\n')
        time.sleep(2)

        # Newest file with invalid format
        temporary = tempfile.mkstemp(dir=tempdir)
        fname = temporary[1]
        with open(fname, 'a') as test_file:
            test_file.write('HDR_OBJECTID\n')

        result, msg = self.ephemeral_gbf.is_valid_hestia_source_dir(tempdir)
        rmtree(tempdir, ignore_errors=True)
        self.assertEqual(result, False)

    def test_is_valid_hestia_source_dir_valid_dir(self):
        """Tests that is_valid_hestia_source_dir behaves as expected
        when input dir contains 2 files and newest one has a valid format
        """
        tempdir = tempfile.mkdtemp()
        temporary = tempfile.mkstemp(dir=tempdir)
        # Oldest file with invalid format
        fname = temporary[1]
        with open(fname, 'a') as test_file:
            test_file.write('HDR_OBJECTID\n')

        time.sleep(2)

        # Newest file with valid format
        temporary = tempfile.mkstemp(dir=tempdir)
        fname = temporary[1]
        with open(fname, 'a') as test_file:
            test_file.write('HDR_OBJECTID\n')
            test_file.write('DATA_OBJECTID\n')

        result, msg = self.ephemeral_gbf.is_valid_hestia_source_dir(tempdir)
        rmtree(tempdir, ignore_errors=True)
        self.assertEqual(result, True)
        self.assertEqual(msg, fname)
