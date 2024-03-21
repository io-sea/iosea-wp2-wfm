"""Tests that utility routines work as expected.
"""
import os
import time
import unittest
from shutil import which, rmtree
from pathlib import Path
import tempfile
import hashlib

from fastapi import HTTPException
from sqlalchemy.sql import text
from itertools import count

from wfm_api.config.wfm_settings import WFMSettings
from wfm_api.utils.utils import remove_duplicates, find_duplicates
from wfm_api.utils.utils import validate_type, validate_description
from wfm_api.utils.utils import validate_workflow_global, validate_workflow_part
from wfm_api.utils.utils import validate_services_part, validate_steps_part, validate_single_step
from wfm_api.utils.utils import validate_used_services, session_exists, replace_variables
from wfm_api.utils.utils import replace_all_variables, search_session_undefined_variables
from wfm_api.utils.utils import leave_if_session_undefined_variables, leave_if_session_exists
from wfm_api.utils.utils import error_if_session_not_started, generate_access_command
from wfm_api.utils.utils import build_service_name, update_service_name_in_array
from wfm_api.utils.utils import update_service_name_in_workflow_description
from wfm_api.utils.utils import get_paths_from_dasi_cfg_file
from wfm_api.utils.utils import update_service_attributes_in_workflow_description
#from wfm_api.utils.utils import launch_ephemeral_service, stop_ephemeral_service
from wfm_api.utils.utils import delete_all_session_steps_descriptions, store_running_services
from wfm_api.utils.utils import wdf_mandatory_keys, wdf_optional_keys
from wfm_api.utils.utils import workflow_mandatory_keys, workflow_optional_keys
from wfm_api.utils.utils import get_session_list_if_unique, get_session_step_from_name
from wfm_api.utils.utils import all_services_allocated, run_step, all_services_stopped
from wfm_api.utils.utils import count_steps_not_stopped, count_services_not_stopped
from wfm_api.utils.utils import check_and_lock_namespaces, one_service_teardown
from wfm_api.utils.utils import setup_session_fields, setup_service_fields, setup_steps_fields
from wfm_api.utils.utils import get_wfm_step_status, get_rm_step_status, is_valid_file_name
from wfm_api.utils.misc_utils.misc_utils import check_isabspathname, check_issize, is_hestia_path
from wfm_api.utils.misc_utils.misc_utils import check_isabspathdir, remove_file, get_newest_file
from wfm_api.utils.ephemeral_services.slurm_utils import is_lua_based

from wfm_api.utils.database.wfm_database import WFMDatabase, ObjectActivityLogging
from wfm_api.utils.database.wfm_database import Session, Service, Base, NamespaceLock
from wfm_api.utils.database.wfm_database import StepDescription, Step

from wfm_api.utils.errors import UnexistingServiceNameError


# session and service unique indexes by test
session_index = count()
service_index = count()

class TestRemoveDuplicates(unittest.TestCase):
    """Test that the function remove_duplicates behaves as expected.
    """
    def test_remove_duplicates_empty(self):
        """Tests that remove_duplicates behaves as expected when
        empty list is provided"""
        keys = []
        result = remove_duplicates(keys)
        expected_result = []
        self.assertListEqual(result, expected_result)

    def test_remove_duplicates_no_duplicate(self):
        """Tests that remove_duplicates behaves as expected when
        list w/o duplicates is provided"""
        keys = [ 'k2', 'k1', 'k0']
        result = remove_duplicates(keys)
        # remove_duplicates() returns a sorted list
        expected_result = sorted(keys)
        self.assertListEqual(result, expected_result)

    def test_remove_duplicates_duplicates(self):
        """Tests that remove_duplicates behaves as expected when
        list with duplicates is provided"""
        keys = [ 'k2', 'k1', 'k2', 'k3', 'k0', 'k2']
        keys_no_dups = [ 'k1', 'k2', 'k3', 'k0']
        result = remove_duplicates(keys)
        # remove_duplicates() returns a sorted list
        expected_result = sorted(keys_no_dups)
        self.assertListEqual(result, expected_result)


class TestFindDuplicates(unittest.TestCase):
    """Test that the function find_duplicates behaves as expected.
    """
    def test_find_duplicates_empty(self):
        """Tests that find_duplicates behaves as expected when
        empty list is provided"""
        keys = []
        result = find_duplicates(keys)
        expected_result = []
        self.assertListEqual(result, expected_result)

    def test_find_duplicates_no_duplicate(self):
        """Tests that find_duplicates behaves as expected when
        list w/o duplicates is provided"""
        keys = [ 'k2', 'k1', 'k3']
        result = find_duplicates(keys)
        expected_result = []
        self.assertListEqual(result, expected_result)

    def test_find_duplicates_duplicates(self):
        """Tests that find_duplicates behaves as expected when
        list with duplicates is provided"""
        keys = [ 'k2', 'k1', 'k2', 'k3', 'k1', 'k0', 'k2']
        keys_dups = [ 'k1', 'k2' ]
        result = find_duplicates(keys)
        # find_duplicates() returns a sorted list
        expected_result = sorted(keys_dups)
        self.assertListEqual(result, expected_result)


class TestValidateType(unittest.TestCase):
    """Test that the function validate_type behaves as expected.
    """
    def test_validate_type_dict_ok(self):
        """Tests that validate_type behaves as expected when
        a dictionary is tested against dict type."""
        object_to_check = { 'key0': 'value0', 'key1': 'value1'}
        validate_type(object_to_check, dict, 'dictionary_object', 'dictionary', 'fake_file')

    def test_validate_type_dict_ko(self):
        """Tests that validate_type behaves as expected when
        a list is tested against dict type."""
        object_to_check = [ 'k0', 'k1', 'k2']
        object_str = 'object_to_check'
        object_type_str = 'dictionary'
        file_str = 'fake_file'
        expected_status = 404
        expected_detail = f"{object_str} should be declared as a {object_type_str} in {file_str}"

        with self.assertRaises(HTTPException) as ctx_mgr:
            validate_type(object_to_check, dict, object_str, object_type_str, file_str)

        exc = ctx_mgr.exception
        self.assertEqual(exc.status_code, expected_status)
        self.assertEqual(exc.detail, expected_detail)

    def test_validate_type_list_ok(self):
        """Tests that validate_type behaves as expected when
        a list is tested against list type."""
        object_to_check = [ 'k0', 'k1', 'k2' ]
        validate_type(object_to_check, list, 'object_to_check', 'list', 'fake_file')

    def test_validate_type_list_ko(self):
        """Tests that validate_type behaves as expected when
        a dictionary is tested against list type."""
        object_to_check = { 'key0': 'value0', 'key1': 'value1'}
        object_str = 'object_to_check'
        object_type_str = 'list'
        file_str = 'fake_file'
        expected_status = 404
        expected_detail = f"{object_str} should be declared as a {object_type_str} in {file_str}"

        with self.assertRaises(HTTPException) as ctx_mgr:
            validate_type(object_to_check, list, object_str, object_type_str, file_str)

        exc = ctx_mgr.exception
        self.assertEqual(exc.status_code, expected_status)
        self.assertEqual(exc.detail, expected_detail)


class TestValidateDescription(unittest.TestCase):
    """Test that the function validate_description behaves as expected.
    """
    def setUp(self):
        self.mandatory_keys = [ 'm1', 'm2' ]
        self.optional_keys = [ 'o1', 'o2' ]
        self.mandatory1 = [ 'm1' ]
        self.mandatory2 = [ 'm2' ]
        self.unknown = [ 'm3' ]

    def test_validate_description_all(self):
        """Tests that validate_description behaves as expected when
        both mandatory and optional keys are provided"""
        result = validate_description(self.mandatory_keys,
                                      self.optional_keys,
                                      self.mandatory_keys + self.optional_keys)
        expected = ""
        self.assertEqual(result, expected)

    def test_validate_description_no_optional(self):
        """Tests that validate_description behaves as expected when
        no optional key is provided"""
        result = validate_description(self.mandatory_keys,
                                      self.optional_keys,
                                      self.mandatory_keys)
        expected = ""
        self.assertEqual(result, expected)

    def test_validate_description_missing_mandatory(self):
        """Tests that validate_description behaves as expected when
        one mandatory key is missing"""
        result = validate_description(self.mandatory_keys,
                                      self.optional_keys,
                                      self.mandatory1)
        expected = f"Missing key(s) {self.mandatory2} "
        self.assertEqual(result, expected)

    def test_validate_description_extra_mandatory(self):
        """Tests that validate_description behaves as expected when
        one mandatory key is given twice"""
        result = validate_description(self.mandatory_keys,
                                      self.optional_keys,
                                      self.mandatory_keys + self.mandatory2)
        expected = f"Duplicate key(s) {self.mandatory2} "
        self.assertEqual(result, expected)

    def test_validate_description_extra_unknown(self):
        """Tests that validate_description behaves as expected when
        an unknown key is given"""
        result = validate_description(self.mandatory_keys,
                                      self.optional_keys,
                                      self.mandatory_keys + self.unknown)
        expected = f"Extra key(s) {self.unknown} "
        self.assertEqual(result, expected)

    def test_validate_description_missing_extra_mandatory(self):
        """Tests that validate_description behaves as expected when
        one mandatory key is missing and one unknown key is given"""
        result = validate_description(self.mandatory_keys,
                                      self.optional_keys,
                                      self.mandatory1 + self.unknown)
        missing_expected = f"Missing key(s) {self.mandatory2} "
        extra_expected = f"Extra key(s) {self.unknown} "
        self.assertIn(missing_expected, result)
        self.assertIn(extra_expected, result)


class TestValidateWorkflowGlobal(unittest.TestCase):
    """ Test that the function validate_workflow_global behaves as expected.
    """
    def test_validate_workflow_global_all(self):
        """Tests that validate_workflow_global behaves as expected when
        both mandatory and optional keys are provided"""
        try:
            validate_workflow_global("fake_file",
                                     wdf_mandatory_keys + wdf_optional_keys)
        except:   # pylint: disable=bare-except
            self.fail("Encountered an unexpected exception.")

    def test_validate_workflow_global_no_optional(self):
        """Tests that validate_workflow_global behaves as expected when
        no optional keys are provided"""
        try:
            validate_workflow_global("fake_file", wdf_mandatory_keys)
        except:   # pylint: disable=bare-except
            self.fail("Encountered an unexpected exception.")

    def test_validate_workflow_global_missing_mandatory(self):
        """Tests that validate_workflow_global behaves as expected when
        a mandatory key is missing"""
        missing = [ 'workflow' ]
        keys = [ 'services', 'steps' ]
        expected_status = 404
        expected_detail = f"Missing key(s) {missing} "
        expected_detail2 = " in fake_file"
        with self.assertRaises(HTTPException) as ctx_mgr:
            validate_workflow_global("fake_file", keys)

        exc = ctx_mgr.exception
        self.assertEqual(exc.status_code, expected_status)
        self.assertIn(expected_detail, exc.detail)
        self.assertIn(expected_detail2, exc.detail)

    def test_validate_workflow_global_duplicate_mandatory(self):
        """Tests that validate_workflow_global behaves as expected when
        a mandatory key is provided twice"""
        duplicate = [ 'services' ]
        keys = wdf_mandatory_keys + duplicate
        expected_status = 404
        expected_detail = f"Duplicate key(s) {duplicate} "
        expected_detail2 = " in fake_file"
        with self.assertRaises(HTTPException) as ctx_mgr:
            validate_workflow_global("fake_file", keys)

        exc = ctx_mgr.exception
        self.assertEqual(exc.status_code, expected_status)
        self.assertIn(expected_detail, exc.detail)
        self.assertIn(expected_detail2, exc.detail)

    def test_validate_workflow_global_unknown_key(self):
        """Tests that validate_workflow_global behaves as expected when
        an unknown key is provided"""
        unknown = [ 'unknown' ]
        keys = wdf_mandatory_keys + unknown
        expected_status = 404
        expected_detail = f"Extra key(s) {unknown} "
        expected_detail2 = " in fake_file"
        with self.assertRaises(HTTPException) as ctx_mgr:
            validate_workflow_global("fake_file", keys)

        exc = ctx_mgr.exception
        self.assertEqual(exc.status_code, expected_status)
        self.assertIn(expected_detail, exc.detail)
        self.assertIn(expected_detail2, exc.detail)


class TestValidateWorkflowPart(unittest.TestCase):
    """ Test that the function validate_workflow_part behaves as expected.
    """
    def test_validate_workflow_part_all(self):
        """Tests that validate_workflow_part behaves as expected when
        both mandatory and optional keys are provided"""
        try:
            validate_workflow_part("fake_file",
                                   workflow_mandatory_keys + workflow_optional_keys)
        except:   # pylint: disable=bare-except
            self.fail("Encountered an unexpected exception.")

    def test_validate_workflow_part_no_optional(self):
        """Tests that validate_workflow_part behaves as expected when
        no optional keys are provided"""
        try:
            validate_workflow_part("fake_file", workflow_mandatory_keys)
        except:   # pylint: disable=bare-except
            self.fail("Encountered an unexpected exception.")

    def test_validate_workflow_part_missing_mandatory(self):
        """Tests that validate_workflow_part behaves as expected when
        a mandatory key is missing"""
        missing = [ 'name' ]
        # The workflow part has presently a single key
        keys = [ ]
        expected_status = 404
        expected_detail = f"Missing key(s) {missing} "
        with self.assertRaises(HTTPException) as ctx_mgr:
            validate_workflow_part("fake_file", keys)

        exc = ctx_mgr.exception
        self.assertEqual(exc.status_code, expected_status)
        self.assertIn(expected_detail, exc.detail)

    def test_validate_workflow_part_duplicate_mandatory(self):
        """Tests that validate_workflow_part behaves as expected when
        a mandatory key is provided twice"""
        duplicate = [ 'name' ]
        keys = workflow_mandatory_keys + duplicate
        expected_status = 404
        expected_detail = f"Duplicate key(s) {duplicate} "
        with self.assertRaises(HTTPException) as ctx_mgr:
            validate_workflow_part("fake_file", keys)

        exc = ctx_mgr.exception
        self.assertEqual(exc.status_code, expected_status)
        self.assertIn(expected_detail, exc.detail)

    def test_validate_workflow_part_unknown_key(self):
        """Tests that validate_workflow_part behaves as expected when
        an unknown key is provided"""
        unknown = [ 'unknown' ]
        keys = workflow_mandatory_keys + unknown
        expected_status = 404
        expected_detail = f"Extra key(s) {unknown} "
        with self.assertRaises(HTTPException) as ctx_mgr:
            validate_workflow_part("fake_file", keys)

        exc = ctx_mgr.exception
        self.assertEqual(exc.status_code, expected_status)
        self.assertIn(expected_detail, exc.detail)


class TestValidateServicesPart(unittest.TestCase):
    """ Test that the function validate_services_part behaves as expected.
    """
    def setUp(self):
        """Set up the tests
        """
        test_config = Path(__file__).parent.absolute() / "test_data" / "settings.yaml"
        api_settings = WFMSettings.from_yaml(test_config)
        self.job_mgr_commands = api_settings.command

    def test_validate_services_part_all(self):
        """Tests that validate_services_part behaves as expected when
        both mandatory and optional keys are provided"""
        services_all = [
            { 'name': 's1', 'type': 'SBB',
              'attributes': { 'targets': '/target1', 'flavor': 'flavor1', 'datanodes': 2 } },
            { 'name': 's2', 'type': 'NFS',
              'attributes': { 'namespace': '/tmp/ns2.txt', 'mountpoint': '/tmp',
                              'storagesize': '4Gi', 'location': 'L2' } }
        ]
        result = validate_services_part("fake_file", services_all, self.job_mgr_commands)
        # validate_services_part() resturns a sorted list
        expected_result = [ 's1', 's2' ]
        self.assertEqual(result, expected_result)

    def test_validate_services_part_no_optional(self):
        """Tests that validate_services_part behaves as expected when
        no optional keys are provided"""
        services_no_opt = [
            { 'name': 's1', 'type': 'SBB',
              'attributes': { 'targets': '/target1', 'flavor': 'flavor1' } },
            { 'name': 's2', 'type': 'NFS',
              'attributes': { 'namespace': '/tmp/ns2.txt', 'mountpoint': '/tmp',
                              'storagesize': '4Gi' } }
        ]
        result = validate_services_part("fake_file", services_no_opt, self.job_mgr_commands)
        # validate_services_part() resturns a sorted list
        expected_result = [ 's1', 's2' ]
        self.assertEqual(result, expected_result)

    def test_validate_services_part_missing_mandatory_1(self):
        """Tests that validate_services_part behaves as expected when
        a mandatory key is missing at the higher level"""
        services_no_attr = [
            { 'name': 's1', 'type': 'SBB' },
            { 'name': 's2', 'type': 'NFS' }
        ]
        missing = [ 'attributes' ]
        expected_status = 404
        expected_detail = f"Missing key(s) {missing} "
        expected_detail2 = "in services description in fake_file"
        with self.assertRaises(HTTPException) as ctx_mgr:
            validate_services_part("fake_file", services_no_attr, self.job_mgr_commands)

        exc = ctx_mgr.exception
        self.assertEqual(exc.status_code, expected_status)
        self.assertIn(expected_detail, exc.detail)
        self.assertIn(expected_detail2, exc.detail)

    def test_validate_services_part_missing_mandatory_SBB(self):
        """Tests that validate_services_part behaves as expected when
        a mandatory key is missing at the attributes level for SBB
        ephemeral service"""
        services_attr_missing_mandatory = [
            { 'name': 's1', 'type': 'SBB',
              'attributes': { 'targets': '/target1', 'datanodes': 2 } },
            { 'name': 's2', 'type': 'SBB',
              'attributes': { 'flavor': 'flavor2', 'datanodes': 4 } }
        ]
        # Only the 1st error is output
        missing = [ 'flavor' ]
        expected_status = 404
        expected_detail = f"Missing key(s) {missing} "
        expected_detail2 = "for service s1 attributes in fake_file"
        with self.assertRaises(HTTPException) as ctx_mgr:
            validate_services_part("fake_file", services_attr_missing_mandatory,
                                   self.job_mgr_commands)

        exc = ctx_mgr.exception
        self.assertEqual(exc.status_code, expected_status)
        self.assertIn(expected_detail, exc.detail)
        self.assertIn(expected_detail2, exc.detail)

    def test_validate_services_part_missing_mandatory_NFS(self):
        """Tests that validate_services_part behaves as expected when
        a mandatory key is missing at the attributes level for NFS
        ephemeral service"""
        services_attr_missing_mandatory = [
            { 'name': 's1', 'type': 'NFS',
              'attributes': { 'mountpoint': '/tmp', 'storagesize': '2GiB', 'location': 'L1' } },
            { 'name': 's2', 'type': 'NFS',
              'attributes': { 'mountpoint': '/tmp', 'storagesize': '4GiB', 'location': 'L2' } }
        ]
        # Only the 1st error is output
        missing = [ 'namespace' ]
        expected_status = 404
        expected_detail = f"Missing key(s) {missing} "
        expected_detail2 = "for service s1 attributes in fake_file"
        with self.assertRaises(HTTPException) as ctx_mgr:
            validate_services_part("fake_file", services_attr_missing_mandatory,
                                   self.job_mgr_commands)

        exc = ctx_mgr.exception
        self.assertEqual(exc.status_code, expected_status)
        self.assertIn(expected_detail, exc.detail)
        self.assertIn(expected_detail2, exc.detail)

    def test_validate_services_part_duplicate_mandatory_1(self):
        """Tests that validate_services_part behaves as expected when
        a mandatory key is provided twice at the higher level"""
        services_dupl_mandatory = [
            { 'name': 's1', 'type': 'SBB', 'name': 's3',
              'attributes': { 'targets': '/target1', 'flavor': 'flavor1', 'datanodes': 2 } },
            { 'name': 's2', 'type': 'NFS', 'name': 's4',
              'attributes': { 'namespace': '/tmp/ns2.txt', 'mountpoint': '/tmp',
                              'storagesize': '4Gi', 'location': 'L2' } }
        ]
        # if a key is given twice in a dict, the last value is the one taken into account
        expected_result = [ 's3', 's4' ]
        result = validate_services_part("fake_file", services_dupl_mandatory,
                                        self.job_mgr_commands)
        self.assertEqual(result, expected_result)

    def test_validate_services_part_duplicate_mandatory_2(self):
        """Tests that validate_services_part behaves as expected when
        a mandatory key is provided twice at the attributes level"""
        services_dupl_attr = [
            { 'name': 's1', 'type': 'SBB',
              'attributes': {
                  'targets': '/target1',
                  'flavor': 'flavor1',
                  'datanodes': 2,
                  'flavor': 'flavor3' } },
            { 'name': 's2', 'type': 'NFS',
              'attributes': { 'namespace': '/tmp/ns2.txt', 'mountpoint': '/tmp',
                              'storagesize': '4Gi', 'location': 'L2', 'namespace': '/tmp/ns3.txt' } }
        ]
        # if a key is given twice in a dict, the last value is the one taken into account
        expected_result = [ 's1', 's2' ]
        result = validate_services_part("fake_file", services_dupl_attr, self.job_mgr_commands)
        self.assertEqual(result, expected_result)

    def test_validate_services_part_duplicate_optional(self):
        """Tests that validate_services_part behaves as expected when
        an optional key is provided twice"""
        services_dupl_attr = [
            { 'name': 's1', 'type': 'SBB',
              'attributes': { 'targets': '/target1', 'flavor': 'flavor1', 'datanodes': 2,
              'datanodes': 8 } },
            { 'name': 's2', 'type': 'NFS',
              'attributes': { 'namespace': '/tmp/ns2.txt', 'mountpoint': '/tmp',
                              'storagesize': '4Gi', 'location': 'L2' } }
        ]
        # if a key is given twice in a dict, the last value is the one taken into account
        expected_result = [ 's1', 's2' ]
        result = validate_services_part("fake_file", services_dupl_attr, self.job_mgr_commands)
        self.assertEqual(result, expected_result)

    def test_validate_services_part_unknown_key_1(self):
        """Tests that validate_services_part behaves as expected when
        an unknown key is provided at the higher level"""
        services_unknown_1 = [
            { 'name': 's1', 'type': 'SBB', 'unknown': 'unk',
              'attributes': { 'targets': '/target1', 'flavor': 'flavor1', 'datanodes': 2 } },
            { 'name': 's2', 'type': 'NFS',
              'attributes': { 'namespace': '/tmp/ns2.txt', 'mountpoint': '/tmp',
                              'storagesize': '4GiB', 'location': 'L2' } }
        ]
        unknown = [ 'unknown' ]
        expected_status = 404
        expected_detail = f"Extra key(s) {unknown} "
        expected_detail2 = "in services description in fake_file"
        with self.assertRaises(HTTPException) as ctx_mgr:
            validate_services_part("fake_file", services_unknown_1, self.job_mgr_commands)

        exc = ctx_mgr.exception
        self.assertEqual(exc.status_code, expected_status)
        self.assertIn(expected_detail, exc.detail)
        self.assertIn(expected_detail2, exc.detail)

    def test_validate_services_part_unknown_key_SBB(self):
        """Tests that validate_services_part behaves as expected when
        an unknown key is provided at the attributes level for SBB"""
        services_unknown_2 = [
            { 'name': 's1', 'type': 'SBB',
              'attributes': { 'targets': '/target1', 'flavor': 'flavor1', 'datanodes': 2 } },
            { 'name': 's2', 'type': 'SBB',
              'attributes': { 'targets': '/target2', 'unknown': 'unk', 'flavor': 'flavor2',
                              'datanodes': 4 } }
        ]
        unknown = [ 'unknown' ]
        expected_status = 404
        expected_detail = f"Extra key(s) {unknown} "
        expected_detail2 = "for service s2 attributes in fake_file"
        with self.assertRaises(HTTPException) as ctx_mgr:
            validate_services_part("fake_file", services_unknown_2, self.job_mgr_commands)

        exc = ctx_mgr.exception
        self.assertEqual(exc.status_code, expected_status)
        self.assertIn(expected_detail, exc.detail)
        self.assertIn(expected_detail2, exc.detail)

    def test_validate_services_part_unknown_key_NFS(self):
        """Tests that validate_services_part behaves as expected when
        an unknown key is provided at the attributes level for NFS"""
        services_unknown_2 = [
            { 'name': 's1', 'type': 'NFS',
              'attributes': { 'namespace': '/tmp/ns.txt', 'mountpoint': '/tmp',
                              'storagesize': '4Gi', 'location': 'L1' } },
            { 'name': 's2', 'type': 'NFS',
              'attributes': {'unknown': 'unk', 'namespace': '/tmp/ns2.txt', 'mountpoint': '/tmp',
                             'storagesize': '4Gi', 'location': 'L2' } }
        ]
        unknown = [ 'unknown' ]
        expected_status = 404
        expected_detail = f"Extra key(s) {unknown} "
        expected_detail2 = "for service s2 attributes in fake_file"
        with self.assertRaises(HTTPException) as ctx_mgr:
            validate_services_part("fake_file", services_unknown_2, self.job_mgr_commands)

        exc = ctx_mgr.exception
        self.assertEqual(exc.status_code, expected_status)
        self.assertIn(expected_detail, exc.detail)
        self.assertIn(expected_detail2, exc.detail)


class TestValidateSingleStep(unittest.TestCase):
    """ Test that the function validate_single_step behaves as expected.
    """
    def test_validate_single_step_all(self):
        """Tests that validate_single_step behaves as expected when
        both mandatory and optional keys are provided"""
        step_all = {'name': 's1', 'command': 'cmd1',
                    'services': [ { 'name': 'srv1_1', 'datamovers': 'DM1_1' } ],
                    'location': 'L1'}
        try:
            validate_single_step("fake_file", step_all)
        except:   # pylint: disable=bare-except
            self.fail("Encountered an unexpected exception.")

    def test_validate_single_step_no_optional_1(self):
        """Tests that validate_single_step behaves as expected when
        no optional keys are provided at the higher level"""
        step_no_opt_1 = { 'name': 's1', 'command': 'cmd1',
                          'services': [ { 'name': 'srv1_1', 'datamovers': 'DM1_1' } ] }
        try:
            validate_single_step("fake_file", step_no_opt_1)
        except:   # pylint: disable=bare-except
            self.fail("Encountered an unexpected exception.")

    def test_validate_single_step_no_optional_2(self):
        """Tests that validate_single_step behaves as expected when
        no optional keys are provided at the services level"""
        step_no_opt_2 = { 'name': 's1', 'command': 'cmd1',
                            'services': [ { 'name': 'srv1_1' } ],
                            'location': 'L1' }
        try:
            validate_single_step("fake_file", step_no_opt_2)
        except:   # pylint: disable=bare-except
            self.fail("Encountered an unexpected exception.")

    def test_validate_single_step_missing_mandatory_1(self):
        """Tests that validate_single_step behaves as expected when
        a mandatory key is missing at the higher level"""
        step_no_srv = { 'name': 's2', 'command': 'cmd2' }
        missing = [ 'services' ]
        expected_status = 404
        expected_detail = f"Missing key(s) {missing} "
        expected_detail2 = "in steps description in fake_file"
        with self.assertRaises(HTTPException) as ctx_mgr:
            validate_single_step("fake_file", step_no_srv)

        exc = ctx_mgr.exception
        self.assertEqual(expected_status, exc.status_code)
        self.assertIn(expected_detail, exc.detail)
        self.assertIn(expected_detail2, exc.detail)

    def test_validate_single_step_missing_mandatory_2(self):
        """Tests that validate_single_step behaves as expected when
        a mandatory key is missing at the services level"""
        step_srv_missing_mandatory = { 'name': 's1', 'command': 'cmd1',
                                       'services': [ { 'datamovers': 'DM1_1' } ] }
        missing = [ 'name' ]
        expected_status = 404
        expected_detail = f"Missing key(s) {missing} "
        expected_detail2 = "for step s1 services in fake_file"
        with self.assertRaises(HTTPException) as ctx_mgr:
            validate_single_step("fake_file", step_srv_missing_mandatory)

        exc = ctx_mgr.exception
        self.assertEqual(exc.status_code, expected_status)
        self.assertIn(expected_detail, exc.detail)
        self.assertIn(expected_detail2, exc.detail)

    def test_validate_single_step_duplicate_mandatory_1(self):
        """Tests that validate_single_step behaves as expected when
        a mandatory key is provided twice at the higher level"""
        step_dupl_mandatory = { 'name': 's1', 'command': 'cmd1', 'command': 'cmd3',
                                'services': [ { 'name': 'srv1_1' } ],
                                'location': 'L1' }
        # if a key is given twice in a dict, the last value is the one taken into account
        try:
            validate_single_step("fake_file", step_dupl_mandatory)
        except:   # pylint: disable=bare-except
            self.fail("Encountered an unexpected exception.")

    def test_validate_single_step_duplicate_mandatory_2(self):
        """Tests that validate_single_step behaves as expected when
        a mandatory key is provided twice at the services level"""
        step_dupl_srv = { 'name': 's1', 'command': 'cmd1',
                          'services': [ { 'name': 'srv1_1', 'name': 'srv1_4' } ],
                          'location': 'L1' }
        # if a key is given twice in a dict, the last value is the one taken into account
        try:
            validate_single_step("fake_file", step_dupl_srv)
        except:   # pylint: disable=bare-except
            self.fail("Encountered an unexpected exception.")

    def test_validate_single_step_duplicate_optional(self):
        """Tests that validate_single_step behaves as expected when
        an optional key is provided twice at the higher level"""
        step_dupl_optional = { 'name': 's1', 'command': 'cmd1',
                               'services': [ { 'name': 'srv1_1', 'datamovers': 'DM1_1' } ],
                               'location': 'L1' , 'location': 'L3'}
        # if a key is given twice in a dict, the last value is the one taken into account
        try:
            validate_single_step("fake_file", step_dupl_optional)
        except:   # pylint: disable=bare-except
            self.fail("Encountered an unexpected exception.")

    def test_validate_single_step_duplicate_optional_srv(self):
        """Tests that validate_single_step behaves as expected when
        an optional key is provided twice at the services level"""
        step_dupl_optional_srv = { 'name': 's1', 'command': 'cmd1',
                                   'services': [ { 'name': 'srv1_1', 'datamovers': 'DM1_1',
                                                   'datamovers': 'DM1_4' } ],
                                   'location': 'L1' }
        # if a key is given twice in a dict, the last value is the one taken into account
        try:
            validate_single_step("fake_file", step_dupl_optional_srv)
        except:   # pylint: disable=bare-except
            self.fail("Encountered an unexpected exception.")

    def test_validate_single_step_unknown_key_1(self):
        """Tests that validate_single_step behaves as expected when
        an unknown key is provided at the higher level"""
        step_unknown_1 = { 'name': 's1', 'command': 'cmd1', 'unknown': 'unkn',
                           'services': [ { 'name': 'srv1_1', 'datamovers': 'DM1_1' } ],
                           'location': 'L1' }
        unknown = [ 'unknown' ]
        expected_status = 404
        expected_detail = f"Extra key(s) {unknown} "
        expected_detail2 = "in steps description in fake_file"
        with self.assertRaises(HTTPException) as ctx_mgr:
            validate_single_step("fake_file", step_unknown_1)

        exc = ctx_mgr.exception
        self.assertEqual(exc.status_code, expected_status)
        self.assertIn(expected_detail, exc.detail)
        self.assertIn(expected_detail2, exc.detail)

    def test_validate_single_step_unknown_key_2(self):
        """Tests that validate_single_step behaves as expected when
        an unknown key is provided at the attributes level"""
        step_unknown_2 = { 'name': 's1', 'command': 'cmd1',
                           'services': [ { 'name': 'srv1_1', 'datamovers': 'DM1_1',
                                           'unknown': 'unkn' } ],
                           'location': 'L1' }
        unknown = [ 'unknown' ]
        expected_status = 404
        expected_detail = f"Extra key(s) {unknown} "
        expected_detail2 = "for step s1 services in fake_file"
        with self.assertRaises(HTTPException) as ctx_mgr:
            validate_single_step("fake_file", step_unknown_2)

        exc = ctx_mgr.exception
        self.assertEqual(exc.status_code, expected_status)
        self.assertIn(expected_detail, exc.detail)
        self.assertIn(expected_detail2, exc.detail)


class TestValidateStepsPart(unittest.TestCase):
    """ Test that the function validate_steps_part behaves as expected.
    """
    def test_validate_steps_part_all(self):
        """Tests that validate_steps_part behaves as expected when
        both mandatory and optional keys are provided"""
        steps_all = [
            { 'name': 's1', 'command': 'cmd1',
              'services': [ { 'name': 'srv1_1', 'datamovers': 'DM1_1' } ],
              'location': 'L1' },
            { 'name': 's2', 'command': 'cmd2',
              'services': [ { 'name': 'srv2_1', 'datamovers': 'DM2_1' } ],
              'location': 'L2' }
        ]
        try:
            validate_steps_part("fake_file", steps_all)
        except:   # pylint: disable=bare-except
            self.fail("Encountered an unexpected exception.")

    def test_validate_steps_part_no_optional_1(self):
        """Tests that validate_steps_part behaves as expected when
        no optional keys are provided at the higher level"""
        steps_no_opt_1 = [
            { 'name': 's1', 'command': 'cmd1',
              'services': [ { 'name': 'srv1_1', 'datamovers': 'DM1_1' } ] },
            { 'name': 's2', 'command': 'cmd2',
              'services': [ { 'name': 'srv2_1', 'datamovers': 'DM2_1' } ] }
        ]
        try:
            validate_steps_part("fake_file", steps_no_opt_1)
        except:   # pylint: disable=bare-except
            self.fail("Encountered an unexpected exception.")

    def test_validate_steps_part_no_optional_2(self):
        """Tests that validate_steps_part behaves as expected when
        no optional keys are provided at the services level"""
        steps_no_opt_2 = [
            { 'name': 's1', 'command': 'cmd1',
              'services': [ { 'name': 'srv1_1' } ],
              'location': 'L1' },
            { 'name': 's2', 'command': 'cmd2',
              'services': [ { 'name': 'srv2_1' } ],
              'location': 'L2' }
        ]
        try:
            validate_steps_part("fake_file", steps_no_opt_2)
        except:   # pylint: disable=bare-except
            self.fail("Encountered an unexpected exception.")

    def test_validate_steps_part_missing_mandatory_1(self):
        """Tests that validate_steps_part behaves as expected when
        a mandatory key is missing at the higher level"""
        steps_no_srv = [
            { 'name': 's1', 'command': 'cmd1' },
            { 'name': 's2', 'command': 'cmd2' }
        ]
        missing = [ 'services' ]
        expected_status = 404
        expected_detail = f"Missing key(s) {missing} "
        expected_detail2 = "in steps description in fake_file"
        with self.assertRaises(HTTPException) as ctx_mgr:
            validate_steps_part("fake_file", steps_no_srv)

        exc = ctx_mgr.exception
        self.assertEqual(expected_status, exc.status_code)
        self.assertIn(expected_detail, exc.detail)
        self.assertIn(expected_detail2, exc.detail)

    def test_validate_steps_part_missing_mandatory_2(self):
        """Tests that validate_steps_part behaves as expected when
        a mandatory key is missing at the services level"""
        steps_srv_missing_mandatory = [
            { 'name': 's1', 'command': 'cmd1',
              'services': [ { 'datamovers': 'DM1_1' } ] },
            { 'name': 's2', 'command': 'cmd2',
              'services': [ { 'name': 'srv2_1', 'datamovers': 'DM2_1' } ] }
        ]
        missing = [ 'name' ]
        expected_status = 404
        expected_detail = f"Missing key(s) {missing} "
        expected_detail2 = "for step s1 services in fake_file"
        with self.assertRaises(HTTPException) as ctx_mgr:
            validate_steps_part("fake_file", steps_srv_missing_mandatory)

        exc = ctx_mgr.exception
        self.assertEqual(exc.status_code, expected_status)
        self.assertIn(expected_detail, exc.detail)
        self.assertIn(expected_detail2, exc.detail)

    def test_validate_steps_part_duplicate_mandatory_1(self):
        """Tests that validate_steps_part behaves as expected when
        a mandatory key is provided twice at the higher level"""
        steps_dupl_mandatory = [
            { 'name': 's1', 'command': 'cmd1', 'command': 'cmd3',
              'services': [ { 'name': 'srv1_1' } ],
              'location': 'L1' },
            { 'name': 's2', 'command': 'cmd2',
              'services': [ { 'name': 'srv2_1' } ],
              'location': 'L2' }
        ]
        # if a key is given twice in a dict, the last value is the one taken into account
        try:
            validate_steps_part("fake_file", steps_dupl_mandatory)
        except:   # pylint: disable=bare-except
            self.fail("Encountered an unexpected exception.")

    def test_validate_steps_part_duplicate_mandatory_2(self):
        """Tests that validate_steps_part behaves as expected when
        a mandatory key is provided twice at the services level"""
        steps_dupl_srv = [
            { 'name': 's1', 'command': 'cmd1',
              'services': [ { 'name': 'srv1_1', 'name': 'srv1_4' } ],
              'location': 'L1' },
            { 'name': 's2', 'command': 'cmd2',
              'services': [ { 'name': 'srv2_1' } ],
              'location': 'L2' }
        ]
        # if a key is given twice in a dict, the last value is the one taken into account
        try:
            validate_steps_part("fake_file", steps_dupl_srv)
        except:   # pylint: disable=bare-except
            self.fail("Encountered an unexpected exception.")

    def test_validate_steps_part_duplicate_optional(self):
        """Tests that validate_steps_part behaves as expected when
        an optional key is provided twice at the higher level"""
        steps_dupl_optional = [
            { 'name': 's1', 'command': 'cmd1',
              'services': [ { 'name': 'srv1_1', 'datamovers': 'DM1_1' } ],
              'location': 'L1' , 'location': 'L3'},
            { 'name': 's2', 'command': 'cmd2',
              'services': [ { 'name': 'srv2_1', 'datamovers': 'DM2_1' } ],
              'location': 'L2' }
        ]
        # if a key is given twice in a dict, the last value is the one taken into account
        try:
            validate_steps_part("fake_file", steps_dupl_optional)
        except:   # pylint: disable=bare-except
            self.fail("Encountered an unexpected exception.")

    def test_validate_steps_part_duplicate_optional_srv(self):
        """Tests that validate_steps_part behaves as expected when
        an optional key is provided twice at the services level"""
        steps_dupl_optional_srv = [
            { 'name': 's1', 'command': 'cmd1',
              'services': [ { 'name': 'srv1_1', 'datamovers': 'DM1_1', 'datamovers': 'DM1_4' } ],
              'location': 'L1' },
            { 'name': 's2', 'command': 'cmd2',
              'services': [ { 'name': 'srv2_1', 'datamovers': 'DM2_1' } ],
              'location': 'L2' }
        ]
        # if a key is given twice in a dict, the last value is the one taken into account
        try:
            validate_steps_part("fake_file", steps_dupl_optional_srv)
        except:   # pylint: disable=bare-except
            self.fail("Encountered an unexpected exception.")

    def test_validate_steps_part_unknown_key_1(self):
        """Tests that validate_steps_part behaves as expected when
        an unknown key is provided at the higher level"""
        steps_unknown_1 = [
            { 'name': 's1', 'command': 'cmd1', 'unknown': 'unkn',
              'services': [ { 'name': 'srv1_1', 'datamovers': 'DM1_1' } ],
              'location': 'L1' },
            { 'name': 's2', 'command': 'cmd2',
              'services': [ { 'name': 'srv2_1', 'datamovers': 'DM2_1' } ],
              'location': 'L2' }
        ]
        unknown = [ 'unknown' ]
        expected_status = 404
        expected_detail = f"Extra key(s) {unknown} "
        expected_detail2 = "in steps description in fake_file"
        with self.assertRaises(HTTPException) as ctx_mgr:
            validate_steps_part("fake_file", steps_unknown_1)

        exc = ctx_mgr.exception
        self.assertEqual(exc.status_code, expected_status)
        self.assertIn(expected_detail, exc.detail)
        self.assertIn(expected_detail2, exc.detail)

    def test_validate_steps_part_unknown_key_2(self):
        """Tests that validate_steps_part behaves as expected when
        an unknown key is provided at the attributes level"""
        steps_unknown_2 = [
            { 'name': 's1', 'command': 'cmd1',
              'services': [ { 'name': 'srv1_1', 'datamovers': 'DM1_1', 'unknown': 'unkn' } ],
              'location': 'L1' },
            { 'name': 's2', 'command': 'cmd2',
              'services': [ { 'name': 'srv2_1', 'datamovers': 'DM2_1' } ],
              'location': 'L2' }
        ]
        unknown = [ 'unknown' ]
        expected_status = 404
        expected_detail = f"Extra key(s) {unknown} "
        expected_detail2 = "for step s1 services in fake_file"
        with self.assertRaises(HTTPException) as ctx_mgr:
            validate_steps_part("fake_file", steps_unknown_2)

        exc = ctx_mgr.exception
        self.assertEqual(exc.status_code, expected_status)
        self.assertIn(expected_detail, exc.detail)
        self.assertIn(expected_detail2, exc.detail)

    def test_validate_steps_defined_twice(self):
        """Tests that validate_steps_part behaves as expected when
        a step is defined twice"""
        step_twice = [
            { 'name': 's1', 'command': 'cmd1',
              'services': [ { 'name': 'srv1_1', 'datamovers': 'DM1_1' } ],
              'location': 'L1' },
            { 'name': 's1', 'command': 'cmd2',
              'services': [ { 'name': 'srv1_1', 'datamovers': 'DM1_1' } ],
              'location': 'L2' }
        ]
        expected_status = 404
        expected_detail = "Some steps are redefined in fake_file"
        with self.assertRaises(HTTPException) as ctx_mgr:
            validate_steps_part("fake_file", step_twice)

        exc = ctx_mgr.exception
        self.assertEqual(exc.status_code, expected_status)
        self.assertIn(expected_detail, exc.detail)


class TestValidateUsedServices(unittest.TestCase):
    """ Test that the function validate_used_services behaves as expected.
    """
    def test_validate_used_services_all_defined(self):
        """Tests that validate_used_services behaves as expected when
        all steps use services already defined"""
        steps_ok = [
            { 'name': 's1', 'command': 'cmd1', 'services': [ { 'name': 'srv1' } ] },
            { 'name': 's2', 'command': 'cmd2', 'services': [ { 'name': 'srv2' } ] }
        ]
        defined_services = [ 'srv1', 'srv2', 'srv3' ]
        result = validate_used_services("fake_file", defined_services, steps_ok)
        expected_result = [ 'srv1', 'srv2' ]
        self.assertListEqual(sorted(result), sorted(expected_result))

    def test_validate_used_services_none_defined(self):
        """Tests that validate_used_services behaves as expected when
        all steps use services never defined"""
        steps_ko = [
            { 'name': 's1', 'command': 'cmd1', 'services': [ { 'name': 'unknown1' } ] },
            { 'name': 's2', 'command': 'cmd2', 'services': [ { 'name': 'unknown2' } ] }
        ]
        defined_services = [ 'srv1', 'srv2', 'srv3' ]
        used_but_not_defined = [ 'unknown1', 'unknown2' ]
        expected_status = 404
        expected_detail = f"Some services are used but not defined in fake_file: {used_but_not_defined}"
        with self.assertRaises(HTTPException) as ctx_mgr:
            validate_used_services("fake_file", defined_services, steps_ko)

        exc = ctx_mgr.exception
        self.assertEqual(exc.status_code, expected_status)
        self.assertIn(expected_detail, exc.detail)

    def test_validate_used_services_some_defined(self):
        """Tests that validate_used_services behaves as expected when
        only one step uses services never defined"""
        steps_ko = [
            { 'name': 's1', 'command': 'cmd1', 'services': [ { 'name': 'srv1' } ] },
            { 'name': 's2', 'command': 'cmd2', 'services': [ { 'name': 'unknown2' } ] }
        ]
        defined_services = [ 'srv1', 'srv2', 'srv3' ]
        used_but_not_defined = [ 'unknown2' ]
        expected_status = 404
        expected_detail = f"Some services are used but not defined in fake_file: {used_but_not_defined}"
        with self.assertRaises(HTTPException) as ctx_mgr:
            validate_used_services("fake_file", defined_services, steps_ko)

        exc = ctx_mgr.exception
        self.assertEqual(exc.status_code, expected_status)
        self.assertIn(expected_detail, exc.detail)


class TestIsValidFileName(unittest.TestCase):
    """ Test that the function is_valid_file_name behaves as expected.
    """
    def test_is_valid_file_name_fname_empty(self):
        """Tests that is_valid_file_name behaves as expected when
        the input string is empty"""
        input_string = ''
        result, msg = is_valid_file_name(input_string)
        expected_result = False
        expected_msg = 'should not be empty'
        self.assertEqual(result, expected_result)
        self.assertEqual(msg, expected_msg)

    def test_is_valid_file_name_fname_ko(self):
        """Tests that is_valid_file_name behaves as expected when
        the input string does not have a valid format"""
        input_string = 'correct/fname'
        result, msg = is_valid_file_name(input_string)
        expected_result = False
        expected_msg = f"should not contain a '/'"
        self.assertEqual(result, expected_result)
        self.assertEqual(msg, expected_msg)

    def test_is_valid_file_name_fname_ok(self):
        """Tests that is_valid_file_name behaves as expected when
        the input string has a valid format"""
        input_string = 'correct_fname'
        result, msg = is_valid_file_name(input_string)
        expected_result = True
        expected_msg = ''
        self.assertEqual(result, expected_result)
        self.assertEqual(msg, expected_msg)


class TestBuildServiceName(unittest.TestCase):
    """ Test that the function build_service_name behaves as expected.
    """
    def test_build_service_name(self):
        """Tests that build_service_name behaves as expected"""
        uname = 'my_uname'
        ses_name = 'my_sesname'
        srv_name = 'my_srvname'
        result = build_service_name(uname, ses_name, srv_name)
        expected_result = str(uname) + '-' + str(ses_name) + '-' + str(srv_name)
        self.assertEqual(result, expected_result)


class TestUpdateServiceNameInArray(unittest.TestCase):
    """ Test that the function update_service_name_in_array behaves as expected.
    """
    def test_update_service_name_in_array_empty_array(self):
        """Tests that update_service_name_in_array behaves as expected when
        the provided array is empty"""
        uname = 'my_uname'
        ses_name = 'my_sesname'
        srv_names = []
        result = update_service_name_in_array(srv_names, uname, ses_name)
        expected_result = srv_names
        self.assertEqual(result, expected_result)

    def test_update_service_name_in_array(self):
        """Tests that update_service_name_in_array behaves as expected when
        the provided array is not empty"""
        uname = 'my_uname'
        ses_name = 'my_sesname'
        srv_names = ['name1', 'name2', 'name3']
        expected_result = [ str(uname) + '-' + str(ses_name) + '-' + str(srv_names[0]),
                            str(uname) + '-' + str(ses_name) + '-' + str(srv_names[1]),
                            str(uname) + '-' + str(ses_name) + '-' + str(srv_names[2])]
        result = update_service_name_in_array(srv_names, uname, ses_name)
        self.assertEqual(result, expected_result)


class TestUpdateServiceNameInWorkflowDescription(unittest.TestCase):
    """ Test that the function update_service_name_in_workflow_description behaves as expected.
    """
    def test_update_service_name_in_workflow_description_empty_wfd(self):
        """Tests that update_service_name_in_workflow_description behaves as expected when
        the workflow description is empty"""
        uname = 'my_uname'
        ses_name = 'my_sesname'
        workflow_description = {'services': [], 'steps': []}
        result = update_service_name_in_workflow_description(workflow_description, uname, ses_name)
        expected_result = workflow_description
        self.assertEqual(result, expected_result)

    def test_update_service_name_in_workflow_description_empty_services(self):
        """Tests that update_service_name_in_workflow_description behaves as expected when
        the services section is empty in the workflow description"""
        uname = 'my_uname'
        ses_name = 'my_sesname'
        workflow_description = {
            'services': [],
            'steps': [
                { 'name': 'step1', 'command': 'cmd1', 'services': [ { 'name': 'srv1' } ] },
                { 'name': 'step2', 'command': 'cmd2', 'services': [ { 'name': 'srv2' } ] }
            ]
        }
        result = update_service_name_in_workflow_description(workflow_description, uname, ses_name)
        expected_result = {
            'services': [],
            'steps': [
                { 'name': 'step1', 'command': 'cmd1',
                  'services': [ { 'name': str(uname) + '-' + str(ses_name) + '-srv1' } ] },
                { 'name': 'step2', 'command': 'cmd2',
                  'services': [ { 'name': str(uname) + '-' + str(ses_name) + '-srv2' } ] }
            ]
        }
        self.assertEqual(result, expected_result)

    def test_update_service_name_in_workflow_description_empty_steps(self):
        """Tests that update_service_name_in_workflow_description behaves as expected when
        the steps section is empty in the workflow description"""
        uname = 'my_uname'
        ses_name = 'my_sesname'
        workflow_description = {
            'services': [
                { 'name': 'srv1', 'type': 'SBB',
                  'attributes': { 'targets': '/target1', 'flavor': 'flavor1', 'datanodes': 2 } },
                { 'name': 'srv2', 'type': 'SBB',
                  'attributes': { 'targets': '/target2', 'flavor': 'flavor2', 'datanodes': 4 } }
            ],
            'steps': []
        }
        result = update_service_name_in_workflow_description(workflow_description, uname, ses_name)
        expected_result = {
            'services': [
                { 'name': str(uname) + '-' + str(ses_name) + '-srv1', 'type': 'SBB',
                  'attributes': { 'targets': '/target1', 'flavor': 'flavor1', 'datanodes': 2 } },
                { 'name': str(uname) + '-' + str(ses_name) + '-srv2', 'type': 'SBB',
                  'attributes': { 'targets': '/target2', 'flavor': 'flavor2', 'datanodes': 4 } }
            ],
            'steps': []
        }
        self.assertEqual(result, expected_result)

    def test_update_service_name_in_workflow_description(self):
        """Tests that update_service_name_in_workflow_description behaves as expected when
        all the sections are empty in the workflow description"""
        uname = 'my_uname'
        ses_name = 'my_sesname'
        workflow_description = {
            'services': [
                { 'name': 'srv1', 'type': 'SBB',
                  'attributes': { 'targets': '/target1', 'flavor': 'flavor1' } },
                { 'name': 'srv2', 'type': 'SBB',
                  'attributes': { 'targets': '/target2', 'flavor': 'flavor2', 'datanodes': 4 } }
            ],
            'steps': [
                { 'name': 'step1', 'command': 'cmd1', 'services': [ { 'name': 'srv1' } ] },
                { 'name': 'step2', 'command': 'cmd2', 'services': [ { 'name': 'srv2' } ] }
            ]
        }
        result = update_service_name_in_workflow_description(workflow_description, uname, ses_name)
        expected_result = {
            'services': [
                { 'name': str(uname) + '-' + str(ses_name) + '-srv1', 'type': 'SBB',
                  'attributes': { 'targets': '/target1', 'flavor': 'flavor1' } },
                { 'name': str(uname) + '-' + str(ses_name) + '-srv2', 'type': 'SBB',
                  'attributes': { 'targets': '/target2', 'flavor': 'flavor2', 'datanodes': 4 } }
            ],
            'steps': [
                { 'name': 'step1', 'command': 'cmd1',
                  'services': [ { 'name': str(uname) + '-' + str(ses_name) + '-srv1' } ] },
                { 'name': 'step2', 'command': 'cmd2',
                  'services': [ { 'name': str(uname) + '-' + str(ses_name) + '-srv2' } ] }
            ]
        }
        self.assertEqual(result, expected_result)


class TestGetPathsFromDasiCfgFile(unittest.TestCase):
    """ Test that the function get_paths_from_dasi_cfg_file
    behaves as expected.
    """
    def test_get_paths_from_dasi_cfg_file(self):
        """Tests that get_paths_from_dasi_cfg_file behaves as expected 
        """
        result,error_msg=get_paths_from_dasi_cfg_file('/tmp/wdf1_dasi.dasi_config.yaml')
        expected_result = ['/mnt_points/dasi']
        self.assertEqual(result, expected_result)
        self.assertEqual(error_msg, "")

    def test_get_paths_from_dasi_cfg_file_unexistent_file(self):
        """Tests that get_paths_from_dasi_cfg_file behaves as expected 
        with a non existing file
        """
        dasi_cfg_file = '/tmp/unexistent_file'
        result,error_msg=get_paths_from_dasi_cfg_file(dasi_cfg_file)
        expected_error = f"Could not open file {dasi_cfg_file} for reading"
        self.assertEqual(result, [])
        self.assertEqual(error_msg, expected_error)

    def test_get_paths_from_dasi_cfg_file_two_path(self):
        """Tests that get_paths_from_dasi_cfg_file behaves as expected 
        with two paths attributes
        """
        # create a temporary config file
        temporary = tempfile.mkstemp()
        dasi_cfg_file = temporary[1]
        with open(dasi_cfg_file, 'a') as cfg_file:
            cfg_file.write('schema: toto\n')
            cfg_file.write('catalogue: toc\n')
            cfg_file.write('store: file\n')
            cfg_file.write('spaces:\n')
            cfg_file.write('  - roots:\n')
            cfg_file.write('    - path: /tmp/p1\n')
            cfg_file.write('    - path: /tmp/p2\n')
        result,error_msg=get_paths_from_dasi_cfg_file(dasi_cfg_file)
        os.remove(dasi_cfg_file)
        self.assertEqual(result, ['/tmp/p1', '/tmp/p2'])
        self.assertEqual(error_msg, "")

    def test_get_paths_from_dasi_cfg_file_two_spaces(self):
        """Tests that get_paths_from_dasi_cfg_file behaves as expected 
        with two spaces attributes
        """
        # create a temporary config file
        temporary = tempfile.mkstemp()
        dasi_cfg_file = temporary[1]
        with open(dasi_cfg_file, 'a') as cfg_file:
            cfg_file.write('schema: toto\n')
            cfg_file.write('catalogue: toc\n')
            cfg_file.write('store: file\n')
            cfg_file.write('spaces:\n')
            cfg_file.write('  - roots:\n')
            cfg_file.write('    - path: p1\n')
            cfg_file.write('  - roots:\n')
            cfg_file.write('    - path: p2\n')
        result,error_msg=get_paths_from_dasi_cfg_file(dasi_cfg_file)
        expected_error = ("Unsupported number of spaces attribute for DASI "
                          "configuration file, only one space is supported")
        os.remove(dasi_cfg_file)
        self.assertEqual(result, [])
        self.assertEqual(error_msg, expected_error)

    def test_get_paths_from_dasi_cfg_file_relative_path(self):
        """Tests that get_paths_from_dasi_cfg_file behaves as expected 
        with a relative path value for the root.path attribute
        """
        # create a temporary config file
        temporary = tempfile.mkstemp()
        dasi_cfg_file = temporary[1]
        dasi_path = 'p1'
        with open(dasi_cfg_file, 'a') as cfg_file:
            cfg_file.write('schema: toto\n')
            cfg_file.write('catalogue: toc\n')
            cfg_file.write('store: file\n')
            cfg_file.write('spaces:\n')
            cfg_file.write('  - roots:\n')
            cfg_file.write(f'    - path: {dasi_path}\n')
        result,error_msg=get_paths_from_dasi_cfg_file(dasi_cfg_file)
        os.remove(dasi_cfg_file)
        self.assertEqual(result, [])
        expected_error = f"DASI root path ({dasi_path}) is not an absolute pathname"
        self.assertEqual(error_msg, expected_error)


class TestUpdateServiceAttributesInWorkflowDescription(unittest.TestCase):
    """ Test that the function update_service_attributes_in_workflow_description
    behaves as expected.
    """
    def test_update_service_attributes_in_workflow_description_empty_wfd(self):
        """Tests that update_service_attributes_in_workflow_description behaves as expected when
        the workflow description is empty"""
        ses_name = 'sesion0'
        workflow_description = {'services': [], 'steps': []}
        result = update_service_attributes_in_workflow_description(workflow_description, ses_name)
        expected_result = workflow_description
        self.assertEqual(result, expected_result)

    def test_update_service_attributes_in_workflow_description_without_dasi(self):
        """Tests that update_service_attributes_in_workflow_description behaves as expected
        without DASI services the workflow description"""
        ses_name = 'session0'
        workflow_description = {
            'services': [
                { 'name': 'srv1', 'type': 'SBB',
                  'attributes': { 'targets': '/target1', 'flavor': 'flavor1' } },
                { 'name': 'srv2', 'type': 'SBB',
                  'attributes': { 'targets': '/target2', 'flavor': 'flavor2', 'datanodes': 4 } }
            ],
            'steps': [
                { 'name': 'step1', 'command': 'cmd1', 'services': [ { 'name': 'srv1' } ] },
                { 'name': 'step2', 'command': 'cmd2', 'services': [ { 'name': 'srv2' } ] }
            ]
        }
        result = update_service_attributes_in_workflow_description(workflow_description, ses_name)
        self.assertEqual(result, workflow_description)

    def test_update_service_attributes_in_workflow_description_only_dasi(self):
        """Tests that update_service_attributes_in_workflow_description behaves as expected
        with DASI services in the workflow description"""
        ses_name = 'session0'
        workflow_description = {
            'services': [
                { 'name': 'srv1', 'type': 'DASI',
                  'attributes': { 'dasiconfig': '/tmp/wdf1_dasi.dasi_config.yaml',
                                  'namespace': '/tmp/test',
                                  'storagesize': '4GiB' } },
                { 'name': 'srv2', 'type': 'DASI',
                  'attributes': { 'dasiconfig': '/tmp/wdf1_dasi.dasi_config.yaml',
                                  'namespace': 'HESTIA@/tmp/test1',
                                  'storagesize': '1Gi' } }
            ],
            'steps': [
                { 'name': 'step1', 'command': 'cmd1', 'services': [ { 'name': 'srv1' } ] },
                { 'name': 'step2', 'command': 'cmd2', 'services': [ { 'name': 'srv2' } ] }
            ]
        }
        result = update_service_attributes_in_workflow_description(workflow_description, ses_name)
        mountpoint = '/mnt_points/dasi'
        filename = hashlib.sha256(mountpoint.encode('utf-8')).hexdigest()
        expected_result = {
            'services': [
                { 'name': 'srv1', 'type': 'DASI',
                  'attributes': { 'dasiconfig': '/tmp/wdf1_dasi.dasi_config.yaml',
                                  'storagesize': '4GiB',
                                  'mountpoint': mountpoint,
                                  'namespace': f'/tmp/test/{filename}'
                                  } },
                { 'name': 'srv2', 'type': 'DASI',
                  'attributes': { 'dasiconfig': '/tmp/wdf1_dasi.dasi_config.yaml',
                                  'storagesize': '1Gi',
                                  'mountpoint': mountpoint,
                                  'namespace': f'HESTIA@/tmp/test1/{filename}'
                                  } }
            ],
            'steps': [
                { 'name': 'step1', 'command': 'cmd1', 'services': [ { 'name': 'srv1' } ] },
                { 'name': 'step2', 'command': 'cmd2', 'services': [ { 'name': 'srv2' } ] }
            ]
        }
        self.assertEqual(result, expected_result)

    def test_update_service_attributes_in_workflow_description(self):
        """Tests that update_service_attributes_in_workflow_description behaves as expected
        with DASI services in the workflow description"""
        ses_name = 'session0'
        workflow_description = {
            'services': [
                { 'name': 'srv1', 'type': 'DASI',
                  'attributes': { 'dasiconfig': '/tmp/wdf1_dasi.dasi_config.yaml',
                                  'namespace': '/tmp/test',
                                  'storagesize': '4GiB' } },
                { 'name': 'srv2', 'type': 'SBB',
                  'attributes': { 'targets': '/target2', 'flavor': 'flavor2', 'datanodes': 4 } }
            ],
            'steps': [
                { 'name': 'step1', 'command': 'cmd1', 'services': [ { 'name': 'srv1' } ] },
                { 'name': 'step2', 'command': 'cmd2', 'services': [ { 'name': 'srv2' } ] }
            ]
        }
        result = update_service_attributes_in_workflow_description(workflow_description, ses_name)
        mountpoint = '/mnt_points/dasi'
        filename = hashlib.sha256(mountpoint.encode('utf-8')).hexdigest()
        expected_result = {
            'services': [
                { 'name': 'srv1', 'type': 'DASI',
                  'attributes': { 'dasiconfig': '/tmp/wdf1_dasi.dasi_config.yaml',
                                  'namespace': f'/tmp/test/{filename}',
                                  'storagesize': '4GiB',
                                  'mountpoint': mountpoint } },
                { 'name': 'srv2', 'type': 'SBB',
                  'attributes': { 'targets': '/target2', 'flavor': 'flavor2', 'datanodes': 4 } }
            ],
            'steps': [
                { 'name': 'step1', 'command': 'cmd1', 'services': [ { 'name': 'srv1' } ] },
                { 'name': 'step2', 'command': 'cmd2', 'services': [ { 'name': 'srv2' } ] }
            ]
        }
        self.assertEqual(result, expected_result)



class TestReplaceVariables(unittest.TestCase):
    """ Test that the function replace_variables behaves as expected.
    """
    def test_replace_variables_empty_replacement_dict(self):
        """Tests that replace_variables behaves as expected when
        replacement dictionary is empty"""
        input_string = 'This is a fake input string'
        replacements = {}
        result = replace_variables(input_string, replacements)
        expected_result = input_string
        self.assertEqual(result, expected_result)

    def test_replace_variables_empty_input_string(self):
        """Tests that replace_variables behaves as expected when
        input string is empty"""
        input_string = ''
        replacements = {'{{ var1 }}': 'val1', '{{ var2 }}': 'val2'}
        result = replace_variables(input_string, replacements)
        expected_result = input_string
        self.assertEqual(result, expected_result)

    def test_replace_variables_no_replacement(self):
        """Tests that replace_variables behaves as expected when
        the input string does not contain any variable to replace"""
        input_string = 'This is a fake input string that does not contain any variable to replace'
        replacements = {'{{ var1 }}': 'val1', '{{ var2 }}': 'val2'}
        result = replace_variables(input_string, replacements)
        expected_result = input_string
        self.assertEqual(result, expected_result)

    def test_replace_variables_several_variables_to_replace(self):
        """Tests that replace_variables behaves as expected when
        the input string contains sevaral variables to replace"""
        input_string = '{{ var1 }} should be replaced by val1 and {{ var2 }} by val2'
        replacements = {'{{ var1 }}': 'val1', '{{ var2 }}': 'val2', '{{ var3 }}': 'val3'}
        result = replace_variables(input_string, replacements)
        expected_result = 'val1 should be replaced by val1 and val2 by val2'
        self.assertEqual(result, expected_result)

    def test_replace_variables_several_occurences_of_variable_to_replace(self):
        """Tests that replace_variables behaves as expected when
        the input string contains sevaral occurences of the same variable to replace"""
        input_string = ('{{ var1 }} should be replaced by val1 and '
                        '{{ var2 }} by val2 and {{ var1 }} by val1')
        replacements = {'{{ var1 }}': 'val1', '{{ var2 }}': 'val2', '{{ var3 }}': 'val3'}
        result = replace_variables(input_string, replacements)
        expected_result = 'val1 should be replaced by val1 and val2 by val2 and val1 by val1'
        self.assertEqual(result, expected_result)


class TestReplaceAllVariables(unittest.TestCase):
    """ Test that the function replace_all_variables behaves as expected.
    """
    def test_replace_all_variables_empty_replacement_dict(self):
        """Tests that replace_all_variables behaves as expected when
        replacement dictionaries are empty"""
        input_string = 'This is a fake input string'
        predefined_vars = {}
        replacements = {}
        result = replace_all_variables(input_string=input_string,
                                       predefined_vars=predefined_vars,
                                       cmdline_vars=replacements)
        expected_result = input_string
        self.assertEqual(result, expected_result)

    def test_replace_all_variables_empty_replacement_dict_predef(self):
        """Tests that replace_all_variables behaves as expected when
        replacement dictionary is empty and there is a predefined var in the input string"""
        input_string = 'This is a {{ SESSION }} fake input string'
        predefined_vars = { '{{ SESSION }}': 'session0' }
        replacements = {}
        result = replace_all_variables(input_string=input_string,
                                       predefined_vars=predefined_vars,
                                       cmdline_vars=replacements)
        expected_result = 'This is a session0 fake input string'
        self.assertEqual(result, expected_result)

    def test_replace_all_variables_empty_input_string(self):
        """Tests that replace_all_variables behaves as expected when
        input string is empty"""
        input_string = ''
        predefined_vars = { '{{ SESSION }}': 'session0' }
        replacements = {'{{ var1 }}': 'val1', '{{ var2 }}': 'val2'}
        result = replace_all_variables(input_string=input_string,
                                       predefined_vars=predefined_vars,
                                       cmdline_vars=replacements)
        expected_result = input_string
        self.assertEqual(result, expected_result)

    def test_replace_all_variables_no_replacement(self):
        """Tests that replace_all_variables behaves as expected when
        the input string does not contain any variable to replace"""
        input_string = 'This is a fake input string that does not contain any variable to replace'
        predefined_vars = { '{{ SESSION }}': 'session0' }
        replacements = {'{{ var1 }}': 'val1', '{{ var2 }}': 'val2'}
        result = replace_all_variables(input_string=input_string,
                                       predefined_vars=predefined_vars,
                                       cmdline_vars=replacements)
        expected_result = input_string
        self.assertEqual(result, expected_result)

    def test_replace_all_variables_no_replacement_predef(self):
        """Tests that replace_all_variables behaves as expected when
        the input string does not contain any variable to replace and there is
        a predefined variable in the input string"""
        input_string = ('This is a fake input string that does not contain any variable to replace\n'
                        'but that {{ SESSION }} contains a predefined variable')
        predefined_vars = { '{{ SESSION }}': 'session0' }
        replacements = {'{{ var1 }}': 'val1', '{{ var2 }}': 'val2'}
        result = replace_all_variables(input_string=input_string,
                                       predefined_vars=predefined_vars,
                                       cmdline_vars=replacements)
        expected_result = ('This is a fake input string that does not contain any variable to replace\n'
                           'but that session0 contains a predefined variable')
        self.assertEqual(result, expected_result)

    def test_replace_all_variables_redefined_predef(self):
        """Tests that replace_all_variables behaves as expected when
        a predefined variable is redefined on the command line"""
        input_string = '{{ var1 }} should be replaced by val1 and {{ var2 }} by val2 and {{ SESSION }} by session0'
        predefined_vars = { '{{ SESSION }}': 'session0' }
        replacements = {'{{ var1 }}': 'val1', '{{ SESSION }}': 'val2', '{{ var3 }}': 'val3'}
        expected_status = 404
        expected_detail = f"Predefined variables should not be redefined on the command line"
        with self.assertRaises(HTTPException) as ctx_mgr:
            replace_all_variables(input_string=input_string,
                                  predefined_vars=predefined_vars,
                                  cmdline_vars=replacements)
        exc = ctx_mgr.exception
        self.assertEqual(exc.status_code, expected_status)
        self.assertEqual(expected_detail, exc.detail)

    def test_replace_all_variables_several_variables_to_replace(self):
        """Tests that replace_all_variables behaves as expected when
        the input string contains sevaral variables to replace"""
        input_string = '{{ var1 }} should be replaced by val1 and {{ var2 }} by val2 and {{ SESSION }} by session0'
        predefined_vars = { '{{ SESSION }}': 'session0' }
        replacements = {'{{ var1 }}': 'val1', '{{ var2 }}': 'val2', '{{ var3 }}': 'val3'}
        result = replace_all_variables(input_string=input_string,
                                       predefined_vars=predefined_vars,
                                       cmdline_vars=replacements)
        expected_result = 'val1 should be replaced by val1 and val2 by val2 and session0 by session0'
        self.assertEqual(result, expected_result)

    def test_replace_all_variables_several_occurences_of_variable_to_replace(self):
        """Tests that replace_all_variables behaves as expected when
        the input string contains sevaral occurences of the same variable to replace"""
        input_string = ('{{ var1 }} should be replaced by val1 and '
                        '{{ var2 }} by val2 and {{ var1 }} by val1 and {{ SESSION }} by session0')
        predefined_vars = { '{{ SESSION }}': 'session0' }
        replacements = {'{{ var1 }}': 'val1', '{{ var2 }}': 'val2', '{{ var3 }}': 'val3'}
        result = replace_all_variables(input_string=input_string,
                                       predefined_vars=predefined_vars,
                                       cmdline_vars=replacements)
        expected_result = ('val1 should be replaced by val1 and val2 by val2 and val1 by val1 '
                          'and session0 by session0')
        self.assertEqual(result, expected_result)


class TestSearchSessionUndefinedVariables(unittest.TestCase):
    """ Test that the function search_session_undefined_variables behaves as expected.
    """
    def test_search_session_undefined_variables_empty_input_string(self):
        """Tests that search_session_undefined_variables behaves as expected when
        input string is empty"""
        input_string = ''
        result = search_session_undefined_variables(workflow_description=input_string)
        expected_result = []
        self.assertListEqual(result, expected_result)

    def test_search_session_undefined_variables_undefs_remaining(self):
        """Tests that search_session_undefined_variables behaves as expected when
        some undefined variables remain in the string"""
        input_string = ('{{ var1 }} should be replaced by val1\nand {{ var2 }} by val2\nsteps:\n'
                        'yet anoyher undefined {{ var3 }}')
        result = search_session_undefined_variables(workflow_description=input_string)
        expected_result = ['{{ var1 }}', '{{ var2 }}', '{{ var3 }}']
        self.assertListEqual(expected_result, result)

    def test_search_session_undefined_variables_no_undefs_remaining(self):
        """Tests that search_session_undefined_variables behaves as expected when
        no undefined variables remain in the string (except in the steps part)"""
        input_string = ('var1 should be replaced by val1\nand var2 by val2\nsteps:\n'
                        '  command: \"yet anoyher undefined {{ var3 }}\"')
        result = search_session_undefined_variables(workflow_description=input_string)
        expected_result = []
        self.assertListEqual(expected_result, result)


class TestLeaveIfSessionUndefinedVariables(unittest.TestCase):
    """ Test that the function leave_if_session_undefined_variables behaves as expected.
    """
    def test_leave_if_session_undefined_variables_empty_input_string(self):
        """Tests that leave_if_session_undefined_variables behaves as expected when
        input string is empty"""
        input_string = ''
        leave_if_session_undefined_variables(workflow_description=input_string)

    def test_leave_if_session_undefined_variables_undefs_remaining(self):
        """Tests that leave_if_session_undefined_variables behaves as expected when
        some undefined variables remain in the string"""
        input_string = ('{{ var1 }} should be replaced by val1\nand {{ var2 }} by val2\nsteps:\n'
                        'yet anoyher undefined {{ var3 }}')
        expected_status = 404
        undefined_variables = ['{{ var1 }}', '{{ var2 }}', '{{ var3 }}']
        expected_detail = ("Session part of the WDF contains undefined variables: "
                          f"{undefined_variables}")
        with self.assertRaises(HTTPException) as ctx_mgr:
            leave_if_session_undefined_variables(workflow_description=input_string)
        exc = ctx_mgr.exception
        self.assertEqual(exc.status_code, expected_status)
        self.assertEqual(expected_detail, exc.detail)

    def test_leave_if_session_undefined_variables_no_undefs_remaining(self):
        """Tests that leave_if_session_undefined_variables behaves as expected when
        no undefined variables remain in the string (except in the steps part)"""
        input_string = ('var1 should be replaced by val1\nand var2 by val2\nsteps:\n'
                        '  command: \"yet anoyher undefined {{ var3 }}\"')
        leave_if_session_undefined_variables(workflow_description=input_string)


#@unittest.skip("skipping TestLaunchEphemeralService")
#class TestLaunchEphemeralService(unittest.TestCase):
#    """ Test that the function launch_ephemeral_service behaves as expected.
#    """
#    def setUp(self):
#
#    def tearDown(self):
#
#    def test_validate_used_services(self):
#
#
#@unittest.skip("skipping TestStopEphemeralService")
#class TestStopEphemeralService(unittest.TestCase):
#    """ Test that the function stop_ephemeral_service behaves as expected.
#    """
#    def setUp(self):
#
#    def tearDown(self):
#
#    def test_validate_used_services(self):
#
#


class TestStoreRunningServices(unittest.TestCase):
    """ Test that the function store_running_services behaves as expected.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        """
        self.wfm_db_mock = WFMDatabase(':memory:')
        Base.metadata.create_all(bind=self.wfm_db_mock.engine)

    def test_store_running_services_no_running_service(self):
        """Tests that store_running_services behaves as expected when
        there is no running service"""
        services = [
                { 'name': 'srv1', 'type': 'SBB',
                  'attributes': { 'targets': '/target1', 'flavor': 'flavor1', 'datanodes': 2 } },
                { 'name': 'srv2', 'type': 'SBB',
                  'attributes': { 'targets': '/target2', 'flavor': 'flavor2', 'datanodes': 4 } } ]
        running_services = []
        store_running_services(self.wfm_db_mock, services, running_services)
        expected_result = []
        # Check that none of the services was stored
        with self.assertRaises(UnexistingServiceNameError):
            self.wfm_db_mock.get_service_info_from_name('srv1')
        with self.assertRaises(UnexistingServiceNameError):
            self.wfm_db_mock.get_service_info_from_name('srv2')

    def test_store_running_services_some_services_running(self):
        """Tests that store_running_services behaves as expected when
        not all services are running"""
        services = [
                { 'name': 'srv1', 'type': 'SBB',
                  'attributes': { 'targets': '/target1', 'flavor': 'flavor1', 'datanodes': 2 } },
                { 'name': 'srv2', 'type': 'SBB',
                  'attributes': { 'targets': '/target2', 'flavor': 'flavor2', 'datanodes': 4 } },
                { 'name': 'srv3', 'type': 'SBB',
                  'attributes': { 'targets': '/target3', 'flavor': 'flavor3', 'datanodes': 8 } },
                { 'name': 'srv4', 'type': 'SBB',
                  'attributes': { 'targets': '/target4', 'flavor': 'flavor4', 'datanodes': 16 } },
                { 'name': 'srv5', 'type': 'SBB',
                  'attributes': { 'targets': '/target5', 'flavor': 'flavor5', 'datanodes': 32 } }
                ]
        running_services = [
                {'name': services[1]['name'], 'type': services[1]['type'],
                 'status': 'status1', 'jobid': 100, 'location': ''},
                {'name': services[3]['name'], 'type': services[3]['type'],
                 'status': 'status3', 'jobid': 100, 'location': ''} ]
        store_running_services(self.wfm_db_mock, services, running_services)
        # Check that 'srv2' service was stored
        expected_result = [
                {'id': 1, 'session_id': None, 'name': services[1]['name'],
                 'type': services[1]['type'], 'location': '',
                 'targets': services[1]['attributes']['targets'], 'status': running_services[0]['status'],
                 'jobid': running_services[0]['jobid']}]
        result = self.wfm_db_mock.get_service_info_from_name(services[1]['name'])
        self.assertListEqual(result, expected_result)

        # Check that 'srv4' service was stored
        expected_result = [
                {'id': 2, 'session_id': None, 'name': services[3]['name'],
                 'type': services[3]['type'], 'location': '',
                 'targets': services[3]['attributes']['targets'], 'status': running_services[1]['status'],
                 'jobid': running_services[1]['jobid']}]
        result = self.wfm_db_mock.get_service_info_from_name(services[3]['name'])
        self.assertListEqual(result, expected_result)

        # Check that 'srv1', 'srv3' and 'srv5' services were not stored
        with self.assertRaises(UnexistingServiceNameError):
            self.wfm_db_mock.get_service_info_from_name(services[0]['name'])
            self.wfm_db_mock.get_service_info_from_name(services[2]['name'])
            self.wfm_db_mock.get_service_info_from_name(services[4]['name'])

        self.wfm_db_mock.delete_service(services[1]['name'])
        self.wfm_db_mock.delete_service(services[3]['name'])

    def test_store_running_services_all_services_running(self):
        """Tests that store_running_services behaves as expected when
        not all services are running"""
        services = [
                { 'name': 'srv1', 'type': 'SBB',
                  'attributes': { 'targets': '/target1', 'flavor': 'flavor1', 'datanodes': 2 } },
                { 'name': 'srv2', 'type': 'SBB',
                  'attributes': { 'targets': '/target2', 'flavor': 'flavor2', 'datanodes': 4 } } ]
        running_services = [
                {'name': services[0]['name'], 'type': services[0]['type'],
                 'status': 'status0', 'jobid': 100, 'location': ''},
                {'name': services[1]['name'], 'type': services[1]['type'],
                 'status': 'status1', 'jobid': 100, 'location': ''} ]
        store_running_services(self.wfm_db_mock, services, running_services)

        # Check that 'srv1' service was stored
        expected_result = [
                {'id': 1, 'session_id': None, 'name': services[0]['name'],
                 'type': services[0]['type'], 'location': '',
                 'targets': services[0]['attributes']['targets'], 'status': running_services[0]['status'],
                 'jobid': running_services[0]['jobid']}]
        result = self.wfm_db_mock.get_service_info_from_name(services[0]['name'])
        self.assertListEqual(result, expected_result)

        # Check that 'srv2' service was stored
        expected_result = [
                {'id': 2, 'session_id': None, 'name': services[1]['name'],
                 'type': services[1]['type'], 'location': '',
                 'targets': services[1]['attributes']['targets'], 'status': running_services[1]['status'],
                 'jobid': running_services[1]['jobid']}]
        result = self.wfm_db_mock.get_service_info_from_name(services[1]['name'])
        self.assertListEqual(result, expected_result)

        self.wfm_db_mock.delete_service(services[0]['name'])
        self.wfm_db_mock.delete_service(services[1]['name'])


class TestSessionExists(unittest.TestCase):
    """ Test that the function session_exists behaves as expected.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        """
        self.wfm_db_mock = WFMDatabase(':memory:')
        Base.metadata.create_all(bind=self.wfm_db_mock.engine)

        self.session_name = f"ses{next(session_index)}"
        session1 = Session(name=self.session_name, workflow_name="wkf1", user_name='user',
                           start_time=123, end_time=123, status='starting')
        service_name = f"srv{next(session_index)}"
        service1 = Service(session_id=session1, name=service_name, service_type='SBB',
                           targets='/tmp', flavor='small', datanodes=4,
                           start_time=123, end_time=123, status='allocated')
        session1.services = [service1]
        stepd1 = StepDescription(name='stp1', session_id=session1, command='cmd1',
                                 service_id=service1)
        session1.step_descriptions = [stepd1]
        service1.step_descriptions = [stepd1]
        self.wfm_db_mock.dbsession.add(service1)
        self.wfm_db_mock.dbsession.add(session1)
        self.wfm_db_mock.dbsession.add(stepd1)
        self.wfm_db_mock.dbsession.commit()
        self.wfm_db_mock.dbsession.refresh(service1)
        self.wfm_db_mock.dbsession.refresh(session1)
        self.wfm_db_mock.dbsession.refresh(stepd1)

    def tearDown(self):
        self.wfm_db_mock.dbsession.close()

    def test_session_exists_when_session_exists(self):
        """Tests that session_exists behaves as expected when
        when the session is in the Session DB"""
        result = session_exists(self.wfm_db_mock, self.session_name, 'wkf1')
        self.assertEqual(result, 1)

    def test_session_exists_when_session_doesnt_exist(self):
        """Tests that session_exists behaves as expected when
        when the session is not in the Session DB"""
        result = session_exists(self.wfm_db_mock, 'unknown', 'wkf1')
        self.assertEqual(result, 0)


class TestLeaveIfSessionExists(unittest.TestCase):
    """ Test that the function leave_if_session_exists behaves as expected.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        """
        self.wfm_db_mock = WFMDatabase(':memory:')
        Base.metadata.create_all(bind=self.wfm_db_mock.engine)

        self.session_name = f"ses{next(session_index)}"
        session1 = Session(name=self.session_name, workflow_name="wkf1",
                           start_time=123, end_time=123, status='starting')
        service_name = f"srv{next(session_index)}"
        service1 = Service(session_id=session1, name=service_name, service_type='SBB',
                           targets='/tmp', flavor='small', datanodes=4,
                           start_time=123, end_time=123, status='allocated')
        session1.services = [service1]
        stepd1 = StepDescription(name='stp1', session_id=session1, command='cmd1',
                                 service_id=service1)
        session1.step_descriptions = [stepd1]
        service1.step_descriptions = [stepd1]
        self.wfm_db_mock.dbsession.add(service1)
        self.wfm_db_mock.dbsession.add(session1)
        self.wfm_db_mock.dbsession.add(stepd1)
        self.wfm_db_mock.dbsession.commit()
        self.wfm_db_mock.dbsession.refresh(service1)
        self.wfm_db_mock.dbsession.refresh(session1)
        self.wfm_db_mock.dbsession.refresh(stepd1)

    def tearDown(self):
        self.wfm_db_mock.dbsession.close()

    def test_leave_if_session_exists_when_session_exists(self):
        """Tests that leave_if_session_exists behaves as expected when
        when the session is in the Session DB"""
        expected_status = 404
        sname = self.session_name
        wname = 'wkf1'
        expected_detail = f"{sname} session (workflow {wname}) is already started"
        with self.assertRaises(HTTPException) as ctx_mgr:
            leave_if_session_exists(self.wfm_db_mock, sname, wname)
        exc = ctx_mgr.exception
        self.assertEqual(exc.status_code, expected_status)
        self.assertEqual(expected_detail, exc.detail)

    def test_leave_if_session_exists_when_session_doesnt_exist(self):
        """Tests that leave_if_session_exists behaves as expected when
        when the session is not in the Session DB"""
        leave_if_session_exists(self.wfm_db_mock, 'unknown', 'wkf1')


class TestLeaveIfSessionNotStarted(unittest.TestCase):
    """ Test that the function error_if_session_not_started behaves as expected.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        """
        self.wfm_db_mock = WFMDatabase(':memory:')
        Base.metadata.create_all(bind=self.wfm_db_mock.engine)

        session_name = f"ses{next(session_index)}"
        session1 = Session(name=session_name, workflow_name="wkf1",
                           start_time=123, end_time=123, status='starting')
        service_name = f"srv{next(session_index)}"
        service1 = Service(session_id=session1, name=service_name, service_type='SBB',
                           targets='/tmp', flavor='small', datanodes=4,
                           start_time=123, end_time=123, status='allocated')
        session1.services = [service1]
        stepd1 = StepDescription(name='stp1', session_id=session1, command='cmd1',
                                 service_id=service1)
        session1.step_descriptions = [stepd1]
        service1.step_descriptions = [stepd1]
        self.wfm_db_mock.dbsession.add(service1)
        self.wfm_db_mock.dbsession.add(session1)
        self.wfm_db_mock.dbsession.add(stepd1)
        self.wfm_db_mock.dbsession.commit()
        self.wfm_db_mock.dbsession.refresh(service1)
        self.wfm_db_mock.dbsession.refresh(session1)
        self.wfm_db_mock.dbsession.refresh(stepd1)

    def tearDown(self):
        self.wfm_db_mock.dbsession.close()

    def test_error_if_session_not_started_when_session_not_started(self):
        """Tests that error_if_session_not_started behaves as expected when
        when the session is not started"""
        session_name = f"ses{next(session_index)}"
        session2 = Session(name=session_name, workflow_name="wkf2",
                           start_time=123, end_time=123, status='stopping')
        # Session w/o service
        session2.services = []
        self.wfm_db_mock.dbsession.add(session2)
        self.wfm_db_mock.dbsession.commit()
        self.wfm_db_mock.dbsession.refresh(session2)

        sessions = self.wfm_db_mock.get_session_info_from_name(session2.name)
        self.assertEqual(len(sessions), 1)

        expected_status = 404
        expected_detail = f"Session {sessions[0]['name']} not started yet"
        with self.assertRaises(HTTPException) as ctx_mgr:
            error_if_session_not_started(self.wfm_db_mock, sessions[0], None)
        exc = ctx_mgr.exception
        self.assertEqual(exc.status_code, expected_status)
        self.assertEqual(expected_detail, exc.detail)

        self.wfm_db_mock.dbsession.delete(session2)
        self.wfm_db_mock.dbsession.commit()

    def test_error_if_session_not_started_when_session_started(self):
        """Tests that error_if_session_not_started behaves as expected when
        when the session is started"""
        session_name = f"ses{next(session_index)}"
        session3 = Session(name=session_name, workflow_name="wkf3",
                           start_time=123, end_time=123, status='active')
        # Session w/o service
        session3.services = []
        self.wfm_db_mock.dbsession.add(session3)
        self.wfm_db_mock.dbsession.commit()
        self.wfm_db_mock.dbsession.refresh(session3)

        sessions = self.wfm_db_mock.get_session_info_from_name(session3.name)
        self.assertEqual(len(sessions), 1)

        error_if_session_not_started(self.wfm_db_mock, sessions[0], None)

        self.wfm_db_mock.dbsession.delete(session3)
        self.wfm_db_mock.dbsession.commit()


class TestGetSessionListIfUnique(unittest.TestCase):
    """ Test that the function get_session_list_if_unique behaves as expected.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        """
        self.wfm_db_mock = WFMDatabase(':memory:')
        Base.metadata.create_all(bind=self.wfm_db_mock.engine)

        self.session_name = f"ses{next(session_index)}"
        session1 = Session(name=self.session_name, workflow_name="wkf1",
                           start_time=123, end_time=123, status='status_ses1')
        service_name = f"srv{next(session_index)}"
        service1 = Service(session_id=session1, name=service_name, service_type='SBB',
                           targets='/tmp', flavor='small', datanodes=4,
                           start_time=123, end_time=123, status='status_srv1')
        session1.services = [service1]
        stepd1 = StepDescription(name='step1', session_id=session1, command='cmd1',
                                 service_id=service1)
        session1.step_descriptions = [stepd1]
        service1.step_descriptions = [stepd1]

        self.wfm_db_mock.dbsession.add(service1)
        self.wfm_db_mock.dbsession.add(session1)
        self.wfm_db_mock.dbsession.add(stepd1)
        self.wfm_db_mock.dbsession.commit()
        self.wfm_db_mock.dbsession.refresh(service1)
        self.wfm_db_mock.dbsession.refresh(session1)
        self.wfm_db_mock.dbsession.refresh(stepd1)

        self.ses_id1 = session1.id

    def tearDown(self):
        self.wfm_db_mock.dbsession.close()

    def test_get_session_list_if_unique_when_no_session_exists(self):
        """Tests that get_session_list_if_unique behaves as expected when
        when there is no such session in the Session DB"""
        unknown_session = 'unknown_session'
        expected_status = 404
        expected_detail = f"Session {unknown_session} not stored in the WFM DB"
        with self.assertRaises(HTTPException) as ctx_mgr:
            get_session_list_if_unique(self.wfm_db_mock, unknown_session)

        exc = ctx_mgr.exception
        self.assertEqual(exc.status_code, expected_status)
        self.assertEqual(expected_detail, exc.detail)

    def test_get_session_list_if_unique_when_one_session_exists(self):
        """Tests that get_session_list_if_unique behaves as expected when
        when there is a single session that fulfills the condition in the
        Session DB"""
        result = get_session_list_if_unique(self.wfm_db_mock, self.session_name)
        expected_result = [
            {'id': self.ses_id1, 'workflow_name': 'wkf1', 'name': self.session_name,
             'start_time': 123, 'end_time': 123, 'status': 'status_ses1'}]
        self.assertListEqual(result, expected_result)

    def test_get_session_list_if_unique_when_several_sessions_exist(self):
        """Tests that get_session_list_if_unique behaves as expected when
        when there are several such sessions in the Session DB"""
        # Add a 2nd session with the same name
        session2 = Session(name=self.session_name, workflow_name="wkf2",
                           start_time=456, end_time=456, status='status_ses2')
        service_name = f"srv{next(session_index)}"
        service2 = Service(session_id=session2, name=service_name, service_type='SBB',
                           targets='/tmp', flavor='small', datanodes=4,
                           start_time=456, end_time=456, status='status_srv2')
        session2.services = [service2]
        stepd2 = StepDescription(name='step2', session_id=session2, command='cmd2',
                                 service_id=service2)
        session2.step_descriptions = [stepd2]
        service2.step_descriptions = [stepd2]
        self.wfm_db_mock.dbsession.add(service2)
        self.wfm_db_mock.dbsession.add(session2)
        self.wfm_db_mock.dbsession.add(stepd2)
        self.wfm_db_mock.dbsession.commit()
        self.wfm_db_mock.dbsession.refresh(service2)
        self.wfm_db_mock.dbsession.refresh(session2)
        self.wfm_db_mock.dbsession.refresh(stepd2)

        expected_status = 404
        expected_detail = f"Session {self.session_name} is not unique in the WFM DB"
        with self.assertRaises(HTTPException) as ctx_mgr:
            get_session_list_if_unique(self.wfm_db_mock, self.session_name)

        exc = ctx_mgr.exception
        self.assertEqual(exc.status_code, expected_status)
        self.assertEqual(expected_detail, exc.detail)
        self.wfm_db_mock.dbsession.delete(stepd2)
        self.wfm_db_mock.dbsession.delete(session2)
        self.wfm_db_mock.dbsession.delete(service2)
        self.wfm_db_mock.dbsession.commit()


class TestGenerateAccessCommandNoSlurm(unittest.TestCase):
    """ Test that the function generate_access_command behaves as expected
    when there is no slurm installed.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        """
        test_config = Path(__file__).parent.absolute() / "test_data" / "settings.yaml"
        api_settings = WFMSettings.from_yaml(test_config)
        self.job_mgr_commands = api_settings.command
        if is_lua_based(self.job_mgr_commands):
            self.job_submission_prefix = "#BB_LUA "
        else:
            self.job_submission_prefix = ""

        self.wfm_db_mock = WFMDatabase(':memory:')
        Base.metadata.create_all(bind=self.wfm_db_mock.engine)

    def tearDown(self):
        self.wfm_db_mock.dbsession.close()

    def test_generate_access_command_no_service(self):
        """Tests that generate_access_command behaves as expected when
        when the session has no service attached"""
        session_name = f"ses{next(session_index)}"
        session1 = Session(name=session_name, workflow_name="wkf1",
                           start_time=123, end_time=123, status='active')
        session1.services = []
        self.wfm_db_mock.dbsession.add(session1)
        self.wfm_db_mock.dbsession.commit()
        self.wfm_db_mock.dbsession.refresh(session1)

        sessions = self.wfm_db_mock.get_session_info_from_name(session_name)
        self.assertEqual(len(sessions), 1)

        expected_status = 404
        expected_detail = (f"No ephemeral service allocated for session {session_name}. "
                           f"Cannot be accessed.")
        with self.assertRaises(HTTPException) as ctx_mgr:
            generate_access_command(self.wfm_db_mock, sessions[0], self.job_mgr_commands)

        exc = ctx_mgr.exception
        self.assertEqual(exc.status_code, expected_status)
        self.assertEqual(expected_detail, exc.detail)

        self.wfm_db_mock.dbsession.delete(session1)
        self.wfm_db_mock.dbsession.commit()

    def test_generate_access_command_no_service_allocated(self):
        """Tests that generate_access_command behaves as expected when
        when the services attached to the session are not allocated"""
        session_name = f"ses{next(session_index)}"
        session1 = Session(name=session_name, workflow_name="wkf1",
                           start_time=123, end_time=123, status='active')
        service_name = f"srv{next(session_index)}"
        service1 = Service(session_id=session1, name=service_name, service_type='SBB',
                           targets='/tmp', flavor='small', datanodes=4,
                           start_time=456, end_time=456, status='stagingin')
        session1.services = [service1]
        self.wfm_db_mock.dbsession.add(service1)
        self.wfm_db_mock.dbsession.add(session1)
        self.wfm_db_mock.dbsession.commit()
        self.wfm_db_mock.dbsession.refresh(service1)
        self.wfm_db_mock.dbsession.refresh(session1)

        sessions = self.wfm_db_mock.get_session_info_from_name(session_name)
        self.assertEqual(len(sessions), 1)

        expected_status = 404
        expected_detail = (f"No ephemeral service allocated for session {session_name}. "
                           f"Cannot be accessed.")
        with self.assertRaises(HTTPException) as ctx_mgr:
            generate_access_command(self.wfm_db_mock, sessions[0], self.job_mgr_commands)

        exc = ctx_mgr.exception
        self.assertEqual(exc.status_code, expected_status)
        self.assertEqual(expected_detail, exc.detail)

        self.wfm_db_mock.dbsession.delete(service1)
        self.wfm_db_mock.dbsession.delete(session1)
        self.wfm_db_mock.dbsession.commit()


    def test_generate_access_command_two_services_allocated(self):
        """Tests that generate_access_command behaves as expected when
        when 2 services attached to the session are allocated"""
        session_name = f"ses{next(session_index)}"
        session1 = Session(name=session_name, workflow_name="wkf1",
                           start_time=123, end_time=123, status='active')
        service_name = f"srv{next(session_index)}"
        service10 = Service(session_id=session1, name=service_name, service_type='SBB',
                           targets='/tmp', flavor='small', datanodes=4,
                           start_time=456, end_time=456, status='stagedin')
        service_name = f"srv{next(session_index)}"
        service11 = Service(session_id=session1, name=service_name, service_type='SBB',
                           targets='/tmp', flavor='small', datanodes=4,
                           start_time=456, end_time=456, status='allocated')
        service_name = f"srv{next(session_index)}"
        service12 = Service(session_id=session1, name=service_name, service_type='SBB',
                           targets='/tmp', flavor='small', datanodes=4,
                           start_time=456, end_time=456, status='stagingin')
        session1.services = [service10, service11, service12]
        self.wfm_db_mock.dbsession.add(service10)
        self.wfm_db_mock.dbsession.add(service11)
        self.wfm_db_mock.dbsession.add(service12)
        self.wfm_db_mock.dbsession.add(session1)
        self.wfm_db_mock.dbsession.commit()
        self.wfm_db_mock.dbsession.refresh(service10)
        self.wfm_db_mock.dbsession.refresh(service11)
        self.wfm_db_mock.dbsession.refresh(service12)
        self.wfm_db_mock.dbsession.refresh(session1)

        sessions = self.wfm_db_mock.get_session_info_from_name(session_name)
        self.assertEqual(len(sessions), 1)

        expected_status = 404
        expected_detail = "Accessing a session with more that 1 ephemeral service is not supported."
        with self.assertRaises(HTTPException) as ctx_mgr:
            generate_access_command(self.wfm_db_mock, sessions[0], self.job_mgr_commands)

        exc = ctx_mgr.exception
        self.assertEqual(exc.status_code, expected_status)
        self.assertEqual(expected_detail, exc.detail)

        self.wfm_db_mock.dbsession.delete(service10)
        self.wfm_db_mock.dbsession.delete(service11)
        self.wfm_db_mock.dbsession.delete(service12)
        self.wfm_db_mock.dbsession.delete(session1)
        self.wfm_db_mock.dbsession.commit()


    def test_generate_access_command_service_bad_type(self):
        """Tests that generate_access_command behaves as expected when
        when a single service attached to the session is allocated, type unknown"""
        stype = 'UNKNOWN'
        session_name = f"ses{next(session_index)}"
        session1 = Session(name=session_name, workflow_name="wkf1",
                           start_time=123, end_time=123, status='active')
        service_name = f"srv{next(session_index)}"
        service10 = Service(session_id=session1, name=service_name, service_type=stype,
                           targets='/tmp', flavor='small', datanodes=4,
                           start_time=456, end_time=456, status='stagedin')
        service_name = f"srv{next(session_index)}"
        service11 = Service(session_id=session1, name=service_name, service_type='SBB',
                           targets='/tmp', flavor='small', datanodes=4,
                           start_time=456, end_time=456, status='stagingin')
        session1.services = [service10, service11]
        self.wfm_db_mock.dbsession.add(service10)
        self.wfm_db_mock.dbsession.add(service11)
        self.wfm_db_mock.dbsession.add(session1)
        self.wfm_db_mock.dbsession.commit()
        self.wfm_db_mock.dbsession.refresh(service10)
        self.wfm_db_mock.dbsession.refresh(service11)
        self.wfm_db_mock.dbsession.refresh(session1)

        sessions = self.wfm_db_mock.get_session_info_from_name(session1.name)
        self.assertEqual(len(sessions), 1)

        expected_status = 404
        expected_detail = f"Ephemeral service {stype} is not supported. Cannot use it."
        with self.assertRaises(HTTPException) as ctx_mgr:
            generate_access_command(self.wfm_db_mock, sessions[0], self.job_mgr_commands)

        exc = ctx_mgr.exception
        self.assertEqual(exc.status_code, expected_status)
        self.assertEqual(expected_detail, exc.detail)

        self.wfm_db_mock.dbsession.delete(service10)
        self.wfm_db_mock.dbsession.delete(service11)
        self.wfm_db_mock.dbsession.delete(session1)
        self.wfm_db_mock.dbsession.commit()


    def test_generate_access_command_service_ok(self):
        """Tests that generate_access_command behaves as expected when
        when a single service attached to the session is allocated"""
        session_name = f"ses{next(session_index)}"
        session1 = Session(name=session_name, workflow_name="wkf1",
                           start_time=123, end_time=123, status='active')
        service_name = f"srv{next(session_index)}"
        service10 = Service(session_id=session1, name=service_name, service_type='SBB',
                           location='loc10', targets='/tmp', flavor='small', datanodes=4,
                           start_time=456, end_time=456, status='stagedin')
        service_name = f"srv{next(session_index)}"
        service11 = Service(session_id=session1, name=service_name, service_type='SBB',
                           location='loc11', targets='/tmp', flavor='small', datanodes=4,
                           start_time=456, end_time=456, status='stagingin')
        session1.services = [service10, service11]
        self.wfm_db_mock.dbsession.add(service10)
        self.wfm_db_mock.dbsession.add(service11)
        self.wfm_db_mock.dbsession.add(session1)
        self.wfm_db_mock.dbsession.commit()
        self.wfm_db_mock.dbsession.refresh(service10)
        self.wfm_db_mock.dbsession.refresh(service11)
        self.wfm_db_mock.dbsession.refresh(session1)

        sname = session1.name
        sessions = self.wfm_db_mock.get_session_info_from_name(session1.name)
        self.assertEqual(len(sessions), 1)

        expected_result = (f"{self.job_mgr_commands.job_submission_cmd} -J interactive "
                           f"-p {service10.location} -N 1 -n 1 "
                           f"--bb \"{self.job_submission_prefix}{service10.service_type} "
                           f"use_persistent Name={service10.name}\" --pty bash")
        result = generate_access_command(self.wfm_db_mock, sessions[0], self.job_mgr_commands)

        self.assertEqual(expected_result, result)

        self.wfm_db_mock.dbsession.delete(service10)
        self.wfm_db_mock.dbsession.delete(service11)
        self.wfm_db_mock.dbsession.delete(session1)
        self.wfm_db_mock.dbsession.commit()


class TestGenerateAccessCommandSlurm(unittest.TestCase):
    """ Test that the function generate_access_command behaves as expected
    when there is a slurm installed.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        """
        test_config = Path(__file__).parent.absolute() / "test_data" / "slurm_settings.yaml"
        api_settings = WFMSettings.from_yaml(test_config)
        self.job_mgr_commands = api_settings.command

        self.wfm_db_mock = WFMDatabase(':memory:')
        Base.metadata.create_all(bind=self.wfm_db_mock.engine)

    def tearDown(self):
        self.wfm_db_mock.dbsession.close()

    @unittest.skipIf(which('scontrol') is None, "Skipping Access command test")
    def test_generate_access_command_service_slurm_ok(self):
        """Tests that generate_access_command behaves as expected when
        when a single service attached to the session is allocated"""
        if is_lua_based(self.job_mgr_commands):
            self.job_submission_prefix = "#BB_LUA "
        else:
            self.job_submission_prefix = ""
        session_name = f"ses{next(session_index)}"
        session1 = Session(name=session_name, workflow_name="wkf1",
                           start_time=123, end_time=123, status='active')
        service_name = f"srv{next(session_index)}"
        service10 = Service(session_id=session1, name=service_name, service_type='SBB',
                           location='loc10', targets='/tmp', flavor='small', datanodes=4,
                           start_time=456, end_time=456, status='stagedin')
        service_name = f"srv{next(session_index)}"
        service11 = Service(session_id=session1, name=service_name, service_type='SBB',
                           location='loc11', targets='/tmp', flavor='small', datanodes=4,
                           start_time=456, end_time=456, status='stagingin')
        session1.services = [service10, service11]
        self.wfm_db_mock.dbsession.add(service10)
        self.wfm_db_mock.dbsession.add(service11)
        self.wfm_db_mock.dbsession.add(session1)
        self.wfm_db_mock.dbsession.commit()
        self.wfm_db_mock.dbsession.refresh(service10)
        self.wfm_db_mock.dbsession.refresh(service11)
        self.wfm_db_mock.dbsession.refresh(session1)

        sname = session1.name
        sessions = self.wfm_db_mock.get_session_info_from_name(session1.name)
        self.assertEqual(len(sessions), 1)

        expected_result = (f"{self.job_mgr_commands.job_submission_cmd} -J interactive "
                           f"-p {service10.location} -N 1 -n 1 "
                           f"--bb \"{self.job_submission_prefix}{service10.service_type} "
                           f"use_persistent Name={service10.name}\" --pty bash")
        result = generate_access_command(self.wfm_db_mock, sessions[0], self.job_mgr_commands)

        self.assertEqual(expected_result, result)

        self.wfm_db_mock.dbsession.delete(service10)
        self.wfm_db_mock.dbsession.delete(service11)
        self.wfm_db_mock.dbsession.delete(session1)
        self.wfm_db_mock.dbsession.commit()


class TestCheckAndLockNamespaces(unittest.TestCase):
    """ Test that the function check_and_lock_namespaces behaves as expected.
    """
    def setUp(self):
        """Set up the tests
        """
        self.wfm_db_mock = WFMDatabase(':memory:')
        Base.metadata.create_all(bind=self.wfm_db_mock.engine)

        self.ns1 = NamespaceLock(ns_name="ns1", service_name="srv1")
        self.wfm_db_mock.dbsession.add(self.ns1)
        self.wfm_db_mock.dbsession.commit()
        self.wfm_db_mock.dbsession.refresh(self.ns1)

    def tearDown(self):
        self.wfm_db_mock.dbsession.close()

    def test_check_and_lock_namespaces_no_ns(self):
        """Tests that check_and_lock_namespaces behaves as expected when
        there is no namespace key in the services description"""
        services = [
            { 'name': 's1', 'type': 'SBB',
              'attributes': { 'targets': '/target1', 'flavor': 'flavor1', 'datanodes': 2 } },
            { 'name': 's2', 'type': 'SBB',
              'attributes': { 'targets': '/target1', 'flavor': 'flavor1', 'datanodes': 2 } }
        ]
        result = check_and_lock_namespaces(self.wfm_db_mock, services)
        expected_result = ""
        self.assertEqual(result, expected_result)

    def test_check_and_lock_namespaces_single_ns(self):
        """Tests that check_and_lock_namespaces behaves as expected when
        there is no namespace key in the services description"""
        services = [
            { 'name': 's1', 'type': 'SBB',
              'attributes': { 'targets': '/target1', 'flavor': 'flavor1', 'datanodes': 2 } },
            { 'name': 's2', 'type': 'NFS',
              'attributes': { 'namespace': self.ns1.ns_name, 'mountpoint': 'mnt2',
                              'storagesize': 'sz2', 'location': 'L2' } }
        ]
        result = check_and_lock_namespaces(self.wfm_db_mock, services)
        srv_list = [ self.ns1.service_name ]
        expected_result = f"NS {self.ns1.ns_name} already used by other services {srv_list}"
        self.assertEqual(result, expected_result)


class TestAllServicesAllocated(unittest.TestCase):
    """ Test that the function all_services_allocated behaves as expected.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        """
        self.wfm_db_mock = WFMDatabase(':memory:')
        Base.metadata.create_all(bind=self.wfm_db_mock.engine)

        session_name = f"ses{next(session_index)}"
        session1 = Session(name=session_name, workflow_name="wkf1",
                           start_time=123, end_time=123, status='status_ses1')
        service_name = f"srv{next(session_index)}"
        service1 = Service(session_id=session1, name=service_name, service_type='SBB',
                           targets='target1', flavor='flavor1', datanodes=4,
                           start_time=123, end_time=123, status='status_srv1')
        session1.services = [service1]
        session_name = f"ses{next(session_index)}"
        session10 = Session(name=session_name, workflow_name="wkf10",
                            start_time=1230, end_time=1230, status='status_ses1')
        session10.services = []

        self.wfm_db_mock.dbsession.add(service1)
        self.wfm_db_mock.dbsession.add(session1)
        self.wfm_db_mock.dbsession.add(session10)
        self.wfm_db_mock.dbsession.commit()
        self.wfm_db_mock.dbsession.refresh(service1)
        self.wfm_db_mock.dbsession.refresh(session1)
        self.wfm_db_mock.dbsession.refresh(session10)
        self.session_id = session1.id
        self.session_no_srv_id = session10.id

    def tearDown(self):
        self.wfm_db_mock.dbsession.close()

    def test_all_services_allocated_when_no_session_exists(self):
        """Tests that all_services_allocated behaves as expected when
        when there is no such session in the Session DB"""
        result = all_services_allocated(self.wfm_db_mock, 1234)
        self.assertEqual(result, True)

    def test_all_services_allocated_when_no_service_allocated(self):
        """Tests that all_services_allocated behaves as expected when
        when there is no service allocated for this session"""
        result = all_services_allocated(self.wfm_db_mock, self.session_id)
        self.assertEqual(result, False)

    def test_all_services_allocated_when_no_service(self):
        """Tests that all_services_allocated behaves as expected when
        when there is no service for this session"""
        result = all_services_allocated(self.wfm_db_mock, self.session_no_srv_id)
        self.assertEqual(result, True)

    def test_all_services_allocated_when_one_service_allocated(self):
        """Tests that all_services_allocated behaves as expected when
        when only one service is allocated for this session"""
        # Add a service in the allocated state
        service_name = f"srv{next(session_index)}"
        service2 = Service(session_id=self.session_id, name=service_name, service_type='SBB',
                           targets='target2', flavor='flavor2', datanodes=4,
                           start_time=123, end_time=123, status='allocated')
        self.wfm_db_mock.dbsession.add(service2)
        self.wfm_db_mock.dbsession.commit()
        result = all_services_allocated(self.wfm_db_mock, self.session_id)
        self.assertEqual(result, False)
        self.wfm_db_mock.dbsession.delete(service2)
        self.wfm_db_mock.dbsession.commit()

    def test_all_services_allocated_when_one_service_stagedin(self):
        """Tests that all_services_allocated behaves as expected when
        when only one service is stagedin for this session"""
        # Add a service in the stagedin state
        service_name = f"srv{next(session_index)}"
        service2 = Service(session_id=self.session_id, name=service_name, service_type='SBB',
                           targets='target2', flavor='flavor2', datanodes=4,
                           start_time=123, end_time=123, status='stagedin')
        self.wfm_db_mock.dbsession.add(service2)
        self.wfm_db_mock.dbsession.commit()
        result = all_services_allocated(self.wfm_db_mock, self.session_id)
        self.assertEqual(result, False)
        self.wfm_db_mock.dbsession.delete(service2)
        self.wfm_db_mock.dbsession.commit()

    def test_all_services_allocated_when_all_services_allocated(self):
        """Tests that all_services_allocated behaves as expected when
        when all the services are allocated for this session"""
        # Add a new session with its services in the allocated state
        session_name = f"ses{next(session_index)}"
        session2 = Session(name=session_name, workflow_name="wkf2",
                           start_time=123, end_time=123, status='status_ses2')
        service_name = f"srv{next(session_index)}"
        service20 = Service(session_id=session2, name=service_name, service_type='SBB',
                            targets='target20', flavor='flavor20', datanodes=4,
                            start_time=123, end_time=123, status='allocated')
        service_name = f"srv{next(session_index)}"
        service21 = Service(session_id=session2, name=service_name, service_type='SBB',
                            targets='target21', flavor='flavor21', datanodes=4,
                            start_time=123, end_time=123, status='allocated')
        session2.services = [service20, service21]

        self.wfm_db_mock.dbsession.add(service20)
        self.wfm_db_mock.dbsession.add(service21)
        self.wfm_db_mock.dbsession.add(session2)
        self.wfm_db_mock.dbsession.commit()
        self.wfm_db_mock.dbsession.refresh(service20)
        self.wfm_db_mock.dbsession.refresh(service21)
        self.wfm_db_mock.dbsession.refresh(session2)

        result = all_services_allocated(self.wfm_db_mock, session2.id)
        self.assertEqual(result, True)

        self.wfm_db_mock.dbsession.delete(service20)
        self.wfm_db_mock.dbsession.delete(service21)
        self.wfm_db_mock.dbsession.delete(session2)
        self.wfm_db_mock.dbsession.commit()

    def test_all_services_allocated_when_all_services_stagedin(self):
        """Tests that all_services_allocated behaves as expected when
        when all the services are stagedin for this session"""
        # Add a new session with its services in the stagedin state
        session_name = f"ses{next(session_index)}"
        session2 = Session(name=session_name, workflow_name="wkf2",
                           start_time=123, end_time=123, status='status_ses2')
        service_name = f"srv{next(session_index)}"
        service20 = Service(session_id=session2, name=service_name, service_type='SBB',
                            targets='target20', flavor='flavor20', datanodes=4,
                            start_time=123, end_time=123, status='stagedin')
        service_name = f"srv{next(session_index)}"
        service21 = Service(session_id=session2, name=service_name, service_type='SBB',
                            targets='target21', flavor='flavor21', datanodes=4,
                            start_time=123, end_time=123, status='stagedin')
        session2.services = [service20, service21]

        self.wfm_db_mock.dbsession.add(service20)
        self.wfm_db_mock.dbsession.add(service21)
        self.wfm_db_mock.dbsession.add(session2)
        self.wfm_db_mock.dbsession.commit()
        self.wfm_db_mock.dbsession.refresh(service20)
        self.wfm_db_mock.dbsession.refresh(service21)
        self.wfm_db_mock.dbsession.refresh(session2)

        result = all_services_allocated(self.wfm_db_mock, session2.id)
        self.assertEqual(result, True)

        self.wfm_db_mock.dbsession.delete(service20)
        self.wfm_db_mock.dbsession.delete(service21)
        self.wfm_db_mock.dbsession.delete(session2)
        self.wfm_db_mock.dbsession.commit()

    def test_all_services_allocated_when_all_services_allocated_or_stagedin(self):
        """Tests that all_services_allocated behaves as expected when
        when all the services are allocated or stagedin for this session"""
        # Add a new session with 1 service in the allocated state
        # and 1 service in the stagedin state
        session_name = f"ses{next(session_index)}"
        session2 = Session(name=session_name, workflow_name="wkf2",
                           start_time=123, end_time=123, status='status_ses2')
        service_name = f"srv{next(session_index)}"
        service20 = Service(session_id=session2, name=service_name, service_type='SBB',
                            targets='target20', flavor='flavor20', datanodes=4,
                            start_time=123, end_time=123, status='allocated')
        service_name = f"srv{next(session_index)}"
        service21 = Service(session_id=session2, name=service_name, service_type='SBB',
                            targets='target21', flavor='flavor21', datanodes=4,
                            start_time=123, end_time=123, status='stagedin')
        session2.services = [service20, service21]

        self.wfm_db_mock.dbsession.add(service20)
        self.wfm_db_mock.dbsession.add(service21)
        self.wfm_db_mock.dbsession.add(session2)
        self.wfm_db_mock.dbsession.commit()
        self.wfm_db_mock.dbsession.refresh(service20)
        self.wfm_db_mock.dbsession.refresh(service21)
        self.wfm_db_mock.dbsession.refresh(session2)

        result = all_services_allocated(self.wfm_db_mock, session2.id)
        self.assertEqual(result, True)

        self.wfm_db_mock.dbsession.delete(service20)
        self.wfm_db_mock.dbsession.delete(service21)
        self.wfm_db_mock.dbsession.delete(session2)
        self.wfm_db_mock.dbsession.commit()


class TestAllServicesStopped(unittest.TestCase):
    """ Test that the function all_services_stopped behaves as expected.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        """
        self.wfm_db_mock = WFMDatabase(':memory:')
        Base.metadata.create_all(bind=self.wfm_db_mock.engine)

        session_name = f"ses{next(session_index)}"
        session1 = Session(name=session_name, workflow_name="wkf1",
                           start_time=123, end_time=123, status='status_ses1')
        service_name = f"srv{next(session_index)}"
        service1 = Service(session_id=session1, name=service_name, service_type='SBB',
                           targets='target1', flavor='flavor1', datanodes=4,
                           start_time=123, end_time=123, status='status_srv1')
        session1.services = [service1]

        self.wfm_db_mock.dbsession.add(service1)
        self.wfm_db_mock.dbsession.add(session1)
        self.wfm_db_mock.dbsession.commit()
        self.wfm_db_mock.dbsession.refresh(service1)
        self.wfm_db_mock.dbsession.refresh(session1)
        self.session_id = session1.id

    def tearDown(self):
        self.wfm_db_mock.dbsession.close()

    def test_all_services_stopped_when_no_session_exists(self):
        """Tests that all_services_stopped behaves as expected when
        when there is no such session in the Session DB"""
        result = all_services_stopped(self.wfm_db_mock, 1000)
        self.assertEqual(result, True)

    def test_all_services_stopped_when_no_service_stopped(self):
        """Tests that all_services_stopped behaves as expected when
        when there is no service stopped for this session"""
        result = all_services_stopped(self.wfm_db_mock, self.session_id)
        self.assertEqual(result, False)

    def test_all_services_stopped_when_one_service_stopped(self):
        """Tests that all_services_stopped behaves as expected when
        when only one service is stopped for this session"""
        # Add a service in the stopped state
        service_name = f"srv{next(session_index)}"
        service2 = Service(session_id=self.session_id, name=service_name, service_type='SBB',
                           targets='target2', flavor='flavor2', datanodes=4,
                           start_time=123, end_time=123, status='stopped')
        self.wfm_db_mock.dbsession.add(service2)
        self.wfm_db_mock.dbsession.commit()
        result = all_services_stopped(self.wfm_db_mock, self.session_id)
        self.assertEqual(result, False)
        self.wfm_db_mock.dbsession.delete(service2)
        self.wfm_db_mock.dbsession.commit()

    def test_all_services_stopped_when_one_service_stagedout(self):
        """Tests that all_services_stopped behaves as expected when
        when only one service is stagedout for this session"""
        # Add a service in the stagedin state
        service_name = f"srv{next(session_index)}"
        service2 = Service(session_id=self.session_id, name=service_name, service_type='SBB',
                           targets='target2', flavor='flavor2', datanodes=4,
                           start_time=123, end_time=123, status='stagedout')
        self.wfm_db_mock.dbsession.add(service2)
        self.wfm_db_mock.dbsession.commit()
        result = all_services_stopped(self.wfm_db_mock, self.session_id)
        self.assertEqual(result, False)
        self.wfm_db_mock.dbsession.delete(service2)
        self.wfm_db_mock.dbsession.commit()

    def test_all_services_stopped_when_all_services_stopped(self):
        """Tests that all_services_stopped behaves as expected when
        when all the services are stopped for this session"""
        # Add a new session with its services in the stopped state
        session_name = f"ses{next(session_index)}"
        session2 = Session(name=session_name, workflow_name="wkf2",
                           start_time=123, end_time=123, status='status_ses2')
        service_name = f"srv{next(session_index)}"
        service20 = Service(session_id=session2, name=service_name, service_type='SBB',
                            targets='target20', flavor='flavor20', datanodes=4,
                            start_time=123, end_time=123, status='stopped')
        service_name = f"srv{next(session_index)}"
        service21 = Service(session_id=session2, name=service_name, service_type='SBB',
                            targets='target21', flavor='flavor21', datanodes=4,
                            start_time=123, end_time=123, status='stopped')
        session2.services = [service20, service21]

        self.wfm_db_mock.dbsession.add(service20)
        self.wfm_db_mock.dbsession.add(service21)
        self.wfm_db_mock.dbsession.add(session2)
        self.wfm_db_mock.dbsession.commit()
        self.wfm_db_mock.dbsession.refresh(service20)
        self.wfm_db_mock.dbsession.refresh(service21)
        self.wfm_db_mock.dbsession.refresh(session2)

        result = all_services_stopped(self.wfm_db_mock, session2.id)
        self.assertEqual(result, True)

        self.wfm_db_mock.dbsession.delete(service20)
        self.wfm_db_mock.dbsession.delete(service21)
        self.wfm_db_mock.dbsession.delete(session2)
        self.wfm_db_mock.dbsession.commit()

    def test_all_services_stopped_when_all_services_stagedout(self):
        """Tests that all_services_stopped behaves as expected when
        when all the services are stagedout for this session"""
        # Add a new session with its services in the stagedin state
        session_name = f"ses{next(session_index)}"
        session2 = Session(name=session_name, workflow_name="wkf2",
                           start_time=123, end_time=123, status='status_ses2')
        service_name = f"srv{next(session_index)}"
        service20 = Service(session_id=session2, name=service_name, service_type='SBB',
                            targets='target20', flavor='flavor20', datanodes=4,
                            start_time=123, end_time=123, status='stagedout')
        service_name = f"srv{next(session_index)}"
        service21 = Service(session_id=session2, name=service_name, service_type='SBB',
                            targets='target21', flavor='flavor21', datanodes=4,
                            start_time=123, end_time=123, status='stagedout')
        session2.services = [service20, service21]

        self.wfm_db_mock.dbsession.add(service20)
        self.wfm_db_mock.dbsession.add(service21)
        self.wfm_db_mock.dbsession.add(session2)
        self.wfm_db_mock.dbsession.commit()
        self.wfm_db_mock.dbsession.refresh(service20)
        self.wfm_db_mock.dbsession.refresh(service21)
        self.wfm_db_mock.dbsession.refresh(session2)

        result = all_services_stopped(self.wfm_db_mock, session2.id)
        self.assertEqual(result, True)

        self.wfm_db_mock.dbsession.delete(service20)
        self.wfm_db_mock.dbsession.delete(service21)
        self.wfm_db_mock.dbsession.delete(session2)
        self.wfm_db_mock.dbsession.commit()

    def test_all_services_stopped_when_all_services_stopped_or_stagedout(self):
        """Tests that all_services_stopped behaves as expected when
        when all the services are stopped or stagedout for this session"""
        # Add a new session with 1 service in the stopped state
        # and 1 service in the stagedout state
        session_name = f"ses{next(session_index)}"
        session2 = Session(name=session_name, workflow_name="wkf2",
                           start_time=123, end_time=123, status='status_ses2')
        service_name = f"srv{next(session_index)}"
        service20 = Service(session_id=session2, name=service_name, service_type='SBB',
                            targets='target20', flavor='flavor20', datanodes=4,
                            start_time=123, end_time=123, status='stopped')
        service_name = f"srv{next(session_index)}"
        service21 = Service(session_id=session2, name=service_name, service_type='SBB',
                            targets='target21', flavor='flavor21', datanodes=4,
                            start_time=123, end_time=123, status='stagedout')
        session2.services = [service20, service21]

        self.wfm_db_mock.dbsession.add(service20)
        self.wfm_db_mock.dbsession.add(service21)
        self.wfm_db_mock.dbsession.add(session2)
        self.wfm_db_mock.dbsession.commit()
        self.wfm_db_mock.dbsession.refresh(service20)
        self.wfm_db_mock.dbsession.refresh(service21)
        self.wfm_db_mock.dbsession.refresh(session2)

        result = all_services_stopped(self.wfm_db_mock, session2.id)
        self.assertEqual(result, True)

        self.wfm_db_mock.dbsession.delete(service20)
        self.wfm_db_mock.dbsession.delete(service21)
        self.wfm_db_mock.dbsession.delete(session2)
        self.wfm_db_mock.dbsession.commit()


class TestOneServiceTeardown(unittest.TestCase):
    """ Test that the function one_service_teardown behaves as expected.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        """
        self.wfm_db_mock = WFMDatabase(':memory:')
        Base.metadata.create_all(bind=self.wfm_db_mock.engine)

        session_name = f"ses{next(session_index)}"
        session1 = Session(name=session_name, workflow_name="wkf1",
                           start_time=123, end_time=123, status='status_ses1')
        service_name = f"srv{next(session_index)}"
        service1 = Service(session_id=session1, name=service_name, service_type='SBB',
                           targets='target1', flavor='flavor1', datanodes=4,
                           start_time=123, end_time=123, status='status_srv1')
        session1.services = [service1]

        self.wfm_db_mock.dbsession.add(service1)
        self.wfm_db_mock.dbsession.add(session1)
        self.wfm_db_mock.dbsession.commit()
        self.wfm_db_mock.dbsession.refresh(service1)
        self.wfm_db_mock.dbsession.refresh(session1)
        self.session_id = session1.id

    def tearDown(self):
        self.wfm_db_mock.dbsession.close()

    def test_one_service_teardown_when_no_session_exists(self):
        """Tests that one_service_teardown behaves as expected when
        there is no such session in the Session DB"""
        result = one_service_teardown(self.wfm_db_mock, 1000)
        self.assertEqual(result, False)

    def test_one_service_teardown_when_no_service_teardown(self):
        """Tests that one_service_teardown behaves as expected when
        there is no service teardown for this session"""
        result = one_service_teardown(self.wfm_db_mock, self.session_id)
        self.assertEqual(result, False)

    def test_one_service_teardown_when_one_service_teardown(self):
        """Tests that one_service_teardown behaves as expected when
        only one service is teardown for this session"""
        # Add a service in the stopped state
        service_name = f"srv{next(session_index)}"
        service2 = Service(session_id=self.session_id, name=service_name, service_type='SBB',
                           targets='target2', flavor='flavor2', datanodes=4,
                           start_time=123, end_time=123, status='teardown')
        self.wfm_db_mock.dbsession.add(service2)
        self.wfm_db_mock.dbsession.commit()
        result = one_service_teardown(self.wfm_db_mock, self.session_id)
        self.assertEqual(result, True)
        self.wfm_db_mock.dbsession.delete(service2)
        self.wfm_db_mock.dbsession.commit()

    def test_one_service_teardown_when_all_services_teardown(self):
        """Tests that one_service_teardown behaves as expected when
        all the services are teardown for this session"""
        # Add a new session with its services in the stopped state
        session_name = f"ses{next(session_index)}"
        session2 = Session(name=session_name, workflow_name="wkf2",
                           start_time=123, end_time=123, status='status_ses2')
        service_name = f"srv{next(session_index)}"
        service20 = Service(session_id=session2, name=service_name, service_type='SBB',
                            targets='target20', flavor='flavor20', datanodes=4,
                            start_time=123, end_time=123, status='teardown')
        service_name = f"srv{next(session_index)}"
        service21 = Service(session_id=session2, name=service_name, service_type='SBB',
                            targets='target21', flavor='flavor21', datanodes=4,
                            start_time=123, end_time=123, status='teardown')
        session2.services = [service20, service21]

        self.wfm_db_mock.dbsession.add(service20)
        self.wfm_db_mock.dbsession.add(service21)
        self.wfm_db_mock.dbsession.add(session2)
        self.wfm_db_mock.dbsession.commit()
        self.wfm_db_mock.dbsession.refresh(service20)
        self.wfm_db_mock.dbsession.refresh(service21)
        self.wfm_db_mock.dbsession.refresh(session2)

        result = one_service_teardown(self.wfm_db_mock, session2.id)
        self.assertEqual(result, True)

        self.wfm_db_mock.dbsession.delete(service20)
        self.wfm_db_mock.dbsession.delete(service21)
        self.wfm_db_mock.dbsession.delete(session2)
        self.wfm_db_mock.dbsession.commit()


class TestCountServicesNotStopped(unittest.TestCase):
    """ Test that the function count_services_not_stopped behaves as expected.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        """
        test_config = Path(__file__).parent.absolute() / "test_data" / "settings.yaml"
        api_settings = WFMSettings.from_yaml(test_config)
        self.job_mgr_commands = api_settings.command

        self.wfm_db_mock = WFMDatabase(':memory:')
        Base.metadata.create_all(bind=self.wfm_db_mock.engine)

        session_name = f"ses{next(session_index)}"
        self.session1 = Session(name=session_name, workflow_name="wkf1",
                                start_time=123, end_time=123, status='status_ses1')
        service_name = f"srv{next(session_index)}"
        self.service10 = Service(session_id=self.session1, name=service_name, service_type='SBB',
                                 location='location10',
                                 targets='target10', flavor='flavor10', datanodes=4,
                                 start_time=123, end_time=123, status='stopped', jobid=10)
        service_name = f"srv{next(session_index)}"
        self.service11 = Service(session_id=self.session1, name=service_name, service_type='SBB',
                                 location='location11',
                                 targets='target11', flavor='flavor11', datanodes=4,
                                 start_time=123, end_time=123, status='stopped', jobid=11)
        self.session1.services = [self.service10, self.service11]

        self.wfm_db_mock.dbsession.add(self.service10)
        self.wfm_db_mock.dbsession.add(self.service11)
        self.wfm_db_mock.dbsession.add(self.session1)
        self.wfm_db_mock.dbsession.commit()
        self.wfm_db_mock.dbsession.refresh(self.service10)
        self.wfm_db_mock.dbsession.refresh(self.service11)
        self.wfm_db_mock.dbsession.refresh(self.session1)

    def tearDown(self):
        self.wfm_db_mock.dbsession.delete(self.service10)
        self.wfm_db_mock.dbsession.delete(self.service11)
        self.wfm_db_mock.dbsession.delete(self.session1)
        self.wfm_db_mock.dbsession.commit()
        self.wfm_db_mock.dbsession.close()

    def test_count_services_not_stopped_when_no_service(self):
        """Tests that count_services_not_stopped behaves as expected when
        no services in the list"""
        result = count_services_not_stopped(self.wfm_db_mock, True, [], 'w0', 's0',
                                            self.job_mgr_commands)
        self.assertEqual(result, 0)

    def test_count_services_not_stopped_when_all_services_stopped(self):
        """Tests that all_services_allocated behaves as expected when
        when there is no service allocated for this session"""
        services = self.wfm_db_mock.get_services_info_from_session_id(self.session1.id)
        expected_result = [{'id': self.service10.id, 'session_id': self.session1.id,
                            'name': self.service10.name, 'type': self.service10.service_type,
                            'location': self.service10.location,
                            'targets': self.service10.targets, 'status': self.service10.status,
                            'jobid': 10},
                           {'id': self.service11.id, 'session_id': self.session1.id,
                            'name': self.service11.name, 'type': self.service11.service_type,
                            'location': self.service11.location,
                            'targets': self.service11.targets, 'status': self.service11.status,
                            'jobid': 11}]
        self.assertListEqual(services, expected_result)
        result = count_services_not_stopped(self.wfm_db_mock, True, services, 'w0', 's0',
                                            self.job_mgr_commands)
        self.assertEqual(result, 0)


class TestDeleteAllSessionStepsDescriptions(unittest.TestCase):
    """ Test that the function delete_all_session_steps_descriptions behaves as expected.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        """
        self.wfm_db_mock = WFMDatabase(':memory:')
        Base.metadata.create_all(bind=self.wfm_db_mock.engine)

        self.session_id = 3
        stepd1 = StepDescription(session_id=self.session_id, name='step1', command='command1')
        stepd2 = StepDescription(session_id=self.session_id, name='step2', command='command2')
        self.wfm_db_mock.dbsession.add(stepd1)
        self.wfm_db_mock.dbsession.add(stepd2)
        self.wfm_db_mock.dbsession.commit()
        self.wfm_db_mock.dbsession.refresh(stepd1)
        self.wfm_db_mock.dbsession.refresh(stepd2)
        self.stepd_id1 = stepd1.id
        self.stepd_id2 = stepd2.id

    def tearDown(self):
        self.wfm_db_mock.dbsession.close()

    def test_delete_all_steps_descriptions_no_stepd_for_session_id(self):
        """Tests that delete_all_session_steps_descriptions behaves as expected when
        when no step description exists in the StepDescription DB for the session id"""
        delete_all_session_steps_descriptions(self.wfm_db_mock, 123)
        # Check that we didn't generate any log into the DB
        filter = text("object_type == 'step_description'")
        result = self.wfm_db_mock.dbsession.query(ObjectActivityLogging).filter(filter).all()
        self.assertListEqual(result, [])

    def test_delete_all_steps_descriptions_stepd_for_session_id(self):
        """Tests that delete_all_session_steps_descriptions behaves as expected when
        when some step descriptions exist in the StepDescription DB for the session id"""
        delete_all_session_steps_descriptions(self.wfm_db_mock, self.session_id)
        # Check that we generated 2 logs into the DB
        filter = text("object_type == 'step_description'")
        result = self.wfm_db_mock.dbsession.query(ObjectActivityLogging).filter(filter).all()
        expected_result = [{'id': 1, 'object_type': 'step_description',
                            'object_id': self.stepd_id1, 'activity': 'removal'},
                           {'id': 2, 'object_type': 'step_description',
                            'object_id': self.stepd_id2, 'activity': 'removal'}]
        list_dicts = [item.dict() for item in result]
        # we do this instead of assertListEqual because the id might not be '1' since we already
        # added some logs in the init
        for idx in (0, 1):
            self.assertEqual(list_dicts[idx]['object_type'], expected_result[idx]['object_type'])
            self.assertEqual(list_dicts[idx]['object_id'], expected_result[idx]['object_id'])
            self.assertEqual(list_dicts[idx]['activity'], expected_result[idx]['activity'])


class TestRunStep(unittest.TestCase):
    """ Test that the function run_step behaves as expected.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        """
        test_config = Path(__file__).parent.absolute() / "test_data" / "settings.yaml"
        api_settings = WFMSettings.from_yaml(test_config)
        self.job_mgr_commands = api_settings.command

        self.wfm_db_mock = WFMDatabase(':memory:')
        Base.metadata.create_all(bind=self.wfm_db_mock.engine)

        self.session_id = 3
        # 1 step description: service type not supported
        service_name = f"srv{next(session_index)}"
        self.service1 = Service(session_id=self.session_id,
                                name=service_name, service_type='UNSUPPORTED',
                                targets='/tmp', flavor='small', datanodes=4,
                                start_time=123, end_time=123, status='allocated')
        self.stepd1 = StepDescription(session_id=self.session_id, name='step1', command='command1',
                                      service_id=self.service1)
        self.service1.step_descriptions = [ self.stepd1 ]
        self.wfm_db_mock.dbsession.add(self.stepd1)
        self.wfm_db_mock.dbsession.add(self.service1)

        # 1 step description: command does not contain sbatch
        service_name = f"srv{next(session_index)}"
        self.service2 = Service(session_id=self.session_id,
                                name=service_name, service_type='SBB',
                                targets='/tmp', flavor='small', datanodes=4,
                                start_time=123, end_time=123, status='allocated')
        self.stepd2 = StepDescription(session_id=self.session_id, name='step2', command='command2',
                                      service_id=self.service2)
        self.service2.step_descriptions = [ self.stepd2 ]
        self.wfm_db_mock.dbsession.add(self.stepd2)
        self.wfm_db_mock.dbsession.add(self.service2)

        self.wfm_db_mock.dbsession.commit()
        self.wfm_db_mock.dbsession.refresh(self.stepd1)
        self.wfm_db_mock.dbsession.refresh(self.stepd2)
        self.wfm_db_mock.dbsession.refresh(self.service1)
        self.wfm_db_mock.dbsession.refresh(self.service2)

    def tearDown(self):
        self.wfm_db_mock.dbsession.delete(self.stepd1)
        self.wfm_db_mock.dbsession.delete(self.stepd2)
        self.wfm_db_mock.dbsession.delete(self.service1)
        self.wfm_db_mock.dbsession.delete(self.service2)
        self.wfm_db_mock.dbsession.commit()
        self.wfm_db_mock.dbsession.close()

    def test_run_step_when_service_not_supported(self):
        """Tests that run_step behaves as expected when
        the step description references an unsupported service type"""
        expected_status = 404
        expected_detail = (f"Step {self.stepd1.name} uses unsupported ephemeral service "
                           f"{self.service1.name} (type={self.service1.service_type})")
        with self.assertRaises(HTTPException) as ctx_mgr:
            run_step(self.wfm_db_mock, self.stepd1.name, self.stepd1.command, 'w0', 's0',
                     self.service1.id, self.job_mgr_commands)

        exc = ctx_mgr.exception
        self.assertEqual(exc.status_code, expected_status)
        self.assertEqual(expected_detail, exc.detail)


class TestGetRMStepStatusSupportedJobMgr(unittest.TestCase):
    """ Test that the function get_rm_step_status behaves as expected.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        """
        test_config = Path(__file__).parent.absolute() / "test_data" / "slurm_settings.yaml"
        api_settings = WFMSettings.from_yaml(test_config)
        self.job_mgr_commands = api_settings.command
        self.job_manager = api_settings.jobmanager

    def test_get_rm_step_status_starting(self):
        """Tests that get_rm_step_status behaves as expected when
        special status STARTING is provided"""
        result = get_rm_step_status('STARTING', self.job_manager.name, self.job_mgr_commands)
        self.assertEqual(result, 'STARTING')


class TestGetRMStepStatusUnsupportedJobMgr(unittest.TestCase):
    """ Test that the function get_rm_step_status behaves as expected
    When job manager is not supported.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        """
        test_config = Path(__file__).parent.absolute() / "test_data" / "settings.yaml"
        api_settings = WFMSettings.from_yaml(test_config)
        self.job_mgr_commands = api_settings.command
        self.job_manager = api_settings.jobmanager

    def test_get_rm_step_status_starting(self):
        """Tests that get_rm_step_status behaves as expected when
        special status STARTING is provided"""
        result = get_rm_step_status('STARTING', self.job_manager.name, self.job_mgr_commands)
        self.assertEqual(result, 'STARTING')

    def test_get_rm_step_status_running(self):
        """Tests that get_rm_step_status behaves as expected when
        status RUNNING is provided"""
        result = get_rm_step_status('RUNNING', self.job_manager.name, self.job_mgr_commands)
        self.assertEqual(result, 'RUNNING')


class TestGetWFMStepStatusSupportedJobMgr(unittest.TestCase):
    """ Test that the function get_wfm_step_status behaves as expected.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        """
        test_config = Path(__file__).parent.absolute() / "test_data" / "slurm_settings.yaml"
        api_settings = WFMSettings.from_yaml(test_config)
        self.job_mgr_commands = api_settings.command
        self.job_manager = api_settings.jobmanager

    def test_get_wfm_step_status(self):
        """Tests that get_wfm_step_status behaves as expected when
        status CANCELLED and COMPLETED are provided"""
        result = get_wfm_step_status('CANCELLED', self.job_manager.name, self.job_mgr_commands)
        self.assertEqual(result, 'STOPPED')
        result = get_wfm_step_status('COMPLETED', self.job_manager.name, self.job_mgr_commands)
        self.assertEqual(result, 'STOPPED')


class TestGetWFMStepStatusUnsupportedJobMgr(unittest.TestCase):
    """ Test that the function get_wfm_step_status behaves as expected
    When job manager is not supported.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        """
        test_config = Path(__file__).parent.absolute() / "test_data" / "settings.yaml"
        api_settings = WFMSettings.from_yaml(test_config)
        self.job_mgr_commands = api_settings.command
        self.job_manager = api_settings.jobmanager

    def test_get_wfm_step_status(self):
        """Tests that get_wfm_step_status behaves as expected when
        status CANCELLED and COMPLETED are provided"""
        result = get_wfm_step_status('CANCELLED', self.job_manager.name, self.job_mgr_commands)
        self.assertEqual(result, '')
        result = get_wfm_step_status('COMPLETED', self.job_manager.name, self.job_mgr_commands)
        self.assertEqual(result, '')


class TestCountStepsNotStoppedUnsupportedJobManager(unittest.TestCase):
    """ Test that the function count_steps_not_stopped behaves as expected.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        """
        test_config = Path(__file__).parent.absolute() / "test_data" / "settings.yaml"
        api_settings = WFMSettings.from_yaml(test_config)
        self.job_mgr_commands = api_settings.command
        self.job_manager = api_settings.jobmanager

        self.wfm_db_mock = WFMDatabase(':memory:')
        Base.metadata.create_all(bind=self.wfm_db_mock.engine)

        # 1 list with 2 stopped steps
        self.step10 = Step(step_description_id=10, instance_name="step1_1", start_time=10,
                           stop_time=10, status="stopped", progress="Copying 10%", jobid=1)
        self.step11 = Step(step_description_id=10, instance_name="step1_2", start_time=100,
                           stop_time=100, status="stopped", progress="Copying 11%", jobid=10)
        self.wfm_db_mock.dbsession.add(self.step10)
        self.wfm_db_mock.dbsession.add(self.step11)

        # 1 list with no stopped steps
        self.step20 = Step(step_description_id=20, start_time=20, stop_time=20,
                           instance_name="step2_1", status="running", progress="Copying 20%",
                           jobid=2)
        self.step21 = Step(step_description_id=20, start_time=200, stop_time=200,
                           instance_name="step2_2", status="running", progress="Copying 21%",
                           jobid=20)
        self.wfm_db_mock.dbsession.add(self.step20)
        self.wfm_db_mock.dbsession.add(self.step21)

        # 1 list with 1 stopped step and 1 not stop step
        self.step30 = Step(step_description_id=30, start_time=30, stop_time=30,
                           instance_name="step3_1", status="stopped", progress="Copying 30%",
                           jobid=3)
        self.step31 = Step(step_description_id=30, start_time=300, stop_time=300,
                           instance_name="step3_2", status="running", progress="Copying 31%",
                           jobid=30)
        self.wfm_db_mock.dbsession.add(self.step30)
        self.wfm_db_mock.dbsession.add(self.step31)

        self.wfm_db_mock.dbsession.commit()
        self.wfm_db_mock.dbsession.refresh(self.step10)
        self.wfm_db_mock.dbsession.refresh(self.step11)
        self.wfm_db_mock.dbsession.refresh(self.step20)
        self.wfm_db_mock.dbsession.refresh(self.step21)
        self.wfm_db_mock.dbsession.refresh(self.step30)
        self.wfm_db_mock.dbsession.refresh(self.step31)

        # List of stopped steps for each step list
        self.steps1 = [self.step10, self.step11]
        self.steps1_stopped = [self.step10, self.step11]
        self.steps2 = [self.step20, self.step21]
        self.steps2_stopped = []
        self.steps3 = [self.step30, self.step31]
        self.steps3_stopped = [self.step30]

    def tearDown(self):
        self.wfm_db_mock.dbsession.delete(self.step10)
        self.wfm_db_mock.dbsession.delete(self.step11)
        self.wfm_db_mock.dbsession.delete(self.step20)
        self.wfm_db_mock.dbsession.delete(self.step21)
        self.wfm_db_mock.dbsession.delete(self.step30)
        self.wfm_db_mock.dbsession.delete(self.step31)
        self.wfm_db_mock.dbsession.commit()
        self.wfm_db_mock.dbsession.close()

    def test_count_steps_not_stopped_when_no_step_forced(self):
        """Tests that count_steps_not_stopped behaves as expected when
        empty list of steps is passed as param - forced mode"""
        result = count_steps_not_stopped(self.wfm_db_mock, [], True, self.job_manager.name,
                                         self.job_mgr_commands)
        self.assertEqual(result, 0)

    def test_count_steps_not_stopped_when_no_step_not_forced(self):
        """Tests that count_steps_not_stopped behaves as expected when
        empty list of steps is passed as param - not forced mode"""
        result = count_steps_not_stopped(self.wfm_db_mock, [], False, self.job_manager.name,
                                         self.job_mgr_commands)
        self.assertEqual(result, 0)

    def test_count_steps_not_stopped_when_all_steps_stopped_forced(self):
        """Tests that count_steps_not_stopped behaves as expected when
        all steps in list are stopped - forced mode"""
        steps = self.wfm_db_mock.get_steps_info_from_step_description_id(self.step10.step_description_id)
        expected_result = [{'id': self.step10.id, 'status': self.step10.status,
                            'progress': self.step10.progress,
                            'jobid': self.step10.jobid, 'instance_name': self.step10.instance_name,
                            'step_description_id': self.step10.step_description_id},
                           {'id': self.step11.id, 'status': self.step11.status,
                            'progress': self.step11.progress,
                            'jobid': self.step11.jobid, 'instance_name': self.step11.instance_name,
                            'step_description_id': self.step11.step_description_id}]
        self.assertListEqual(steps, expected_result)
        result = count_steps_not_stopped(self.wfm_db_mock, steps, True, self.job_manager.name,
                                         self.job_mgr_commands)
        # Note that since the job manager is not supported (fake) the stopped steps are not
        # accounted for
        expected_count = len(self.steps1)
        self.assertEqual(result, expected_count)

    def test_count_steps_not_stopped_when_all_steps_stopped_not_forced(self):
        """Tests that count_steps_not_stopped behaves as expected when
        all steps in list are stopped - not forced mode"""
        steps = self.wfm_db_mock.get_steps_info_from_step_description_id(self.step10.step_description_id)
        expected_result = [{'id': self.step10.id, 'status': self.step10.status,
                            'progress': self.step10.progress,
                            'jobid': self.step10.jobid, 'instance_name': self.step10.instance_name,
                            'step_description_id': self.step10.step_description_id},
                           {'id': self.step11.id, 'status': self.step11.status,
                            'progress': self.step11.progress,
                            'jobid': self.step11.jobid, 'instance_name': self.step11.instance_name,
                            'step_description_id': self.step11.step_description_id}]
        self.assertListEqual(steps, expected_result)
        result = count_steps_not_stopped(self.wfm_db_mock, steps, False, self.job_manager.name,
                                         self.job_mgr_commands)
        # Note that since the job manager is not supported (fake) the stopped steps are not
        # accounted for
        expected_count = len(self.steps1)
        self.assertEqual(result, expected_count)

    @unittest.skip('Skipping forced stop mode when no step is stopped')
    def test_count_steps_not_stopped_when_no_step_stopped_forced(self):
        """Tests that count_steps_not_stopped behaves as expected when
        no step in list is stopped - forced mode"""
        steps = self.wfm_db_mock.get_steps_info_from_step_description_id(self.step20.step_description_id)
        expected_result = [{'id': self.step20.id, 'status': self.step20.status,
                            'progress': self.step20.progress,
                            'jobid': self.step20.jobid, 'instance_name': self.step20.instance_name,
                            'step_description_id': self.step20.step_description_id},
                           {'id': self.step21.id, 'status': self.step21.status,
                            'progress': self.step21.progress,
                            'jobid': self.step21.jobid, 'instance_name': self.step21.instance_name,
                            'step_description_id': self.step21.step_description_id}]
        self.assertListEqual(steps, expected_result)
        result = count_steps_not_stopped(self.wfm_db_mock, steps, True, self.job_manager.name,
                                         self.job_mgr_commands)
        expected_count = len(self.steps2) - len(self.steps2_stopped)
        self.assertEqual(result, expected_count)

    def test_count_steps_not_stopped_when_no_step_stopped_not_forced(self):
        """Tests that count_steps_not_stopped behaves as expected when
        no step in list is stopped - not forced mode"""
        steps = self.wfm_db_mock.get_steps_info_from_step_description_id(self.step20.step_description_id)
        expected_result = [{'id': self.step20.id, 'status': self.step20.status,
                            'progress': self.step20.progress,
                            'jobid': self.step20.jobid, 'instance_name': self.step20.instance_name,
                            'step_description_id': self.step20.step_description_id},
                           {'id': self.step21.id, 'status': self.step21.status,
                            'progress': self.step21.progress,
                            'jobid': self.step21.jobid, 'instance_name': self.step21.instance_name,
                            'step_description_id': self.step21.step_description_id}]
        self.assertListEqual(steps, expected_result)
        result = count_steps_not_stopped(self.wfm_db_mock, steps, False, self.job_manager.name,
                                         self.job_mgr_commands)
        expected_count = len(self.steps2) - len(self.steps2_stopped)
        self.assertEqual(result, expected_count)

    @unittest.skip('Skipping forced stop mode when part of steps are stopped')
    def test_count_steps_not_stopped_when_part_steps_stopped_forced(self):
        """Tests that count_steps_not_stopped behaves as expected when
        part of the steps in list are stopped - forced mode"""
        steps = self.wfm_db_mock.get_steps_info_from_step_description_id(self.step30.step_description_id)
        expected_result = [{'id': self.step30.id, 'status': self.step30.status,
                            'progress': self.step30.progress,
                            'jobid': self.step30.jobid, 'instance_name': self.step30.instance_name,
                            'step_description_id': self.step30.step_description_id},
                           {'id': self.step31.id, 'status': self.step31.status,
                            'progress': self.step31.progress,
                            'jobid': self.step31.jobid, 'instance_name': self.step31.instance_name,
                            'step_description_id': self.step31.step_description_id}]
        self.assertListEqual(steps, expected_result)
        result = count_steps_not_stopped(self.wfm_db_mock, steps, True, self.job_manager.name,
                                         self.job_mgr_commands)
        expected_count = len(self.steps3) - len(self.steps3_stopped)
        self.assertEqual(result, expected_count)

    def test_count_steps_not_stopped_when_part_steps_stopped_not_forced(self):
        """Tests that count_steps_not_stopped behaves as expected when
        part of the steps in list are stopped - not forced mode"""
        steps = self.wfm_db_mock.get_steps_info_from_step_description_id(self.step30.step_description_id)
        expected_result = [{'id': self.step30.id, 'status': self.step30.status,
                            'progress': self.step30.progress,
                            'jobid': self.step30.jobid, 'instance_name': self.step30.instance_name,
                            'step_description_id': self.step30.step_description_id},
                           {'id': self.step31.id, 'status': self.step31.status,
                            'progress': self.step31.progress,
                            'jobid': self.step31.jobid, 'instance_name': self.step31.instance_name,
                            'step_description_id': self.step31.step_description_id}]
        self.assertListEqual(steps, expected_result)
        result = count_steps_not_stopped(self.wfm_db_mock, steps, False, self.job_manager.name,
                                         self.job_mgr_commands)
        # Note that since the job manager is not supported (fake) the stopped steps are not
        # accounted for
        expected_count = len(self.steps3)
        self.assertEqual(result, expected_count)


class TestCountStepsNotStoppedSupportedJobManager(unittest.TestCase):
    """ Test that the function count_steps_not_stopped behaves as expected
    when Job maanger is supported.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        """
        test_config = Path(__file__).parent.absolute() / "test_data" / "slurm_settings.yaml"
        api_settings = WFMSettings.from_yaml(test_config)
        self.job_mgr_commands = api_settings.command
        self.job_manager = api_settings.jobmanager

        self.wfm_db_mock = WFMDatabase(':memory:')
        Base.metadata.create_all(bind=self.wfm_db_mock.engine)

        # 1 list with 2 stopped steps
        self.step10 = Step(step_description_id=10, instance_name="step1_1", start_time=10,
                           stop_time=10, status="stopped", progress="Copying 10%", jobid=1)
        self.step11 = Step(step_description_id=10, instance_name="step1_2", start_time=100,
                           stop_time=100, status="stopped", progress="Copying 11%", jobid=10)
        self.wfm_db_mock.dbsession.add(self.step10)
        self.wfm_db_mock.dbsession.add(self.step11)

        # 1 list with 1 stopped step and 1 not stop step
        self.step30 = Step(step_description_id=30, start_time=30, stop_time=30,
                           instance_name="step3_1", status="stopped", progress="Copying 30%",
                           jobid=3)
        self.step31 = Step(step_description_id=30, start_time=300, stop_time=300,
                           instance_name="step3_2", status="running", progress="Copying 31%",
                           jobid=30)
        self.wfm_db_mock.dbsession.add(self.step30)
        self.wfm_db_mock.dbsession.add(self.step31)

        self.wfm_db_mock.dbsession.commit()
        self.wfm_db_mock.dbsession.refresh(self.step10)
        self.wfm_db_mock.dbsession.refresh(self.step11)
        self.wfm_db_mock.dbsession.refresh(self.step30)
        self.wfm_db_mock.dbsession.refresh(self.step31)

        # List of stopped steps for each step list
        self.steps1 = [self.step10, self.step11]
        self.steps1_stopped = [self.step10, self.step11]
        self.steps3 = [self.step30, self.step31]
        self.steps3_stopped = [self.step30]

    def tearDown(self):
        self.wfm_db_mock.dbsession.delete(self.step10)
        self.wfm_db_mock.dbsession.delete(self.step11)
        self.wfm_db_mock.dbsession.delete(self.step30)
        self.wfm_db_mock.dbsession.delete(self.step31)
        self.wfm_db_mock.dbsession.commit()
        self.wfm_db_mock.dbsession.close()

    def test_count_steps_not_stopped_when_all_steps_stopped_forced(self):
        """Tests that count_steps_not_stopped behaves as expected when
        all steps in list are stopped - forced mode"""
        steps = self.wfm_db_mock.get_steps_info_from_step_description_id(self.step10.step_description_id)
        expected_result = [{'id': self.step10.id, 'status': self.step10.status,
                            'progress': self.step10.progress,
                            'jobid': self.step10.jobid, 'instance_name': self.step10.instance_name,
                            'step_description_id': self.step10.step_description_id},
                           {'id': self.step11.id, 'status': self.step11.status,
                            'progress': self.step11.progress,
                            'jobid': self.step11.jobid, 'instance_name': self.step11.instance_name,
                            'step_description_id': self.step11.step_description_id}]
        self.assertListEqual(steps, expected_result)
        result = count_steps_not_stopped(self.wfm_db_mock, steps, True, self.job_manager.name,
                                         self.job_mgr_commands)
        expected_count = len(self.steps1) - len(self.steps1_stopped)
        self.assertEqual(result, expected_count)

    def test_count_steps_not_stopped_when_all_steps_stopped_not_forced(self):
        """Tests that count_steps_not_stopped behaves as expected when
        all steps in list are stopped - not forced mode"""
        steps = self.wfm_db_mock.get_steps_info_from_step_description_id(self.step10.step_description_id)
        expected_result = [{'id': self.step10.id, 'status': self.step10.status,
                            'progress': self.step10.progress,
                            'jobid': self.step10.jobid, 'instance_name': self.step10.instance_name,
                            'step_description_id': self.step10.step_description_id},
                           {'id': self.step11.id, 'status': self.step11.status,
                            'progress': self.step11.progress,
                            'jobid': self.step11.jobid, 'instance_name': self.step11.instance_name,
                            'step_description_id': self.step11.step_description_id}]
        self.assertListEqual(steps, expected_result)
        result = count_steps_not_stopped(self.wfm_db_mock, steps, False, self.job_manager.name,
                                         self.job_mgr_commands)
        expected_count = len(self.steps1) - len(self.steps1_stopped)
        self.assertEqual(result, expected_count)

    def test_count_steps_not_stopped_when_part_steps_stopped_not_forced(self):
        """Tests that count_steps_not_stopped behaves as expected when
        part of the steps in list are stopped - not forced mode"""
        steps = self.wfm_db_mock.get_steps_info_from_step_description_id(self.step30.step_description_id)
        expected_result = [{'id': self.step30.id, 'status': self.step30.status,
                            'progress': self.step30.progress,
                            'jobid': self.step30.jobid, 'instance_name': self.step30.instance_name,
                            'step_description_id': self.step30.step_description_id},
                           {'id': self.step31.id, 'status': self.step31.status,
                            'progress': self.step31.progress,
                            'jobid': self.step31.jobid, 'instance_name': self.step31.instance_name,
                            'step_description_id': self.step31.step_description_id}]
        self.assertListEqual(steps, expected_result)
        result = count_steps_not_stopped(self.wfm_db_mock, steps, False, self.job_manager.name,
                                         self.job_mgr_commands)
        expected_count = len(self.steps3) - len(self.steps3_stopped)
        self.assertEqual(result, expected_count)


class TestGetSessionStepFromName(unittest.TestCase):
    """ Test that the function get_session_step_from_name behaves as expected.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        Doing this because one of the tests adds elements to the DB.
        """
        self.wfm_db_mock = WFMDatabase(':memory:')
        Base.metadata.create_all(bind=self.wfm_db_mock.engine)

        # Define 1 session with a single step
        session_name = f"ses{next(session_index)}"
        self.session1 = Session(name=session_name, workflow_name="wkf1",
                                start_time=0, end_time=0, status="status1")
        self.stepd1 = StepDescription(session_id=self.session1, name='step1', command='command1')
        self.step1 = Step(step_description_id=self.stepd1, start_time=123, stop_time=123,
                          status='status1', progress="Copying 1%", jobid=3, instance_name='step1_1')
        self.session1.step_descriptions = [self.stepd1]
        self.stepd1.steps = [self.step1]

        self.wfm_db_mock.dbsession.add(self.session1)
        self.wfm_db_mock.dbsession.add(self.stepd1)
        self.wfm_db_mock.dbsession.add(self.step1)

        # Define another session with a 2 steps
        session_name = f"ses{next(session_index)}"
        self.session2 = Session(name=session_name, workflow_name="wkf2",
                                start_time=0, end_time=0, status="status2")
        self.stepd2 = StepDescription(session_id=self.session2, name='step2', command='command2')
        self.step20 = Step(step_description_id=self.stepd2, start_time=123, stop_time=123,
                           status='status20', progress="Copying 20%", jobid=20,
                           instance_name='step2_1')
        self.step21 = Step(step_description_id=self.stepd2, start_time=123, stop_time=123,
                           status='status21', progress="Copying 21%", jobid=21,
                           instance_name='step2_2')
        self.session2.step_descriptions = [self.stepd2]
        self.stepd2.steps = [self.step20, self.step21]

        self.wfm_db_mock.dbsession.add(self.session2)
        self.wfm_db_mock.dbsession.add(self.stepd2)
        self.wfm_db_mock.dbsession.add(self.step20)
        self.wfm_db_mock.dbsession.add(self.step21)

        self.wfm_db_mock.dbsession.commit()
        self.wfm_db_mock.dbsession.refresh(self.step1)
        self.wfm_db_mock.dbsession.refresh(self.stepd1)
        self.wfm_db_mock.dbsession.refresh(self.session1)
        self.wfm_db_mock.dbsession.refresh(self.step20)
        self.wfm_db_mock.dbsession.refresh(self.step21)
        self.wfm_db_mock.dbsession.refresh(self.stepd2)
        self.wfm_db_mock.dbsession.refresh(self.session2)

    def tearDown(self):
        self.wfm_db_mock.dbsession.delete(self.session1)
        self.wfm_db_mock.dbsession.delete(self.stepd1)
        self.wfm_db_mock.dbsession.delete(self.step1)
        self.wfm_db_mock.dbsession.delete(self.session2)
        self.wfm_db_mock.dbsession.delete(self.stepd2)
        self.wfm_db_mock.dbsession.delete(self.step20)
        self.wfm_db_mock.dbsession.delete(self.step21)
        self.wfm_db_mock.dbsession.commit()
        self.wfm_db_mock.dbsession.close()

    def test_get_session_step_from_name_no_session(self):
        """Tests that getting steps info from session name and step name
        behaves as expected when no session with this name exists in the DB"""
        session_name = 'unknown_session'
        step_name = self.stepd1.name
        expected_status = 404
        expected_detail = f"Session {session_name} not stored in the WFM DB"
        with self.assertRaises(HTTPException) as ctx_mgr:
            get_session_step_from_name(self.wfm_db_mock, session_name, step_name)

        exc = ctx_mgr.exception
        self.assertEqual(exc.status_code, expected_status)
        self.assertEqual(expected_detail, exc.detail)

    def test_get_session_step_from_name_no_step(self):
        """Tests that getting steps info from session name and step name
        behaves as expected when session exists but no step with this name exists
        for this session in the DB"""
        session_name = self.session1.name
        step_name = 'unknown'
        expected_status = 404
        expected_detail = f"Step {step_name} not stored in the WFM DB for session {session_name}"
        with self.assertRaises(HTTPException) as ctx_mgr:
            get_session_step_from_name(self.wfm_db_mock, session_name, step_name)

        exc = ctx_mgr.exception
        self.assertEqual(exc.status_code, expected_status)
        self.assertEqual(expected_detail, exc.detail)

    def test_get_session_step_from_name_single_step(self):
        """Tests that getting steps info from session name and step name
        behaves as expected when a single step exists with this name
        for this session in the DB"""
        result = get_session_step_from_name(self.wfm_db_mock,
                                            self.session1.name, self.stepd1.name)
        expected_result = [{'id': self.step1.id,
                            'instance_name': self.step1.instance_name,
                            'status': self.step1.status,
                            'progress': self.step1.progress,
                            'jobid': self.step1.jobid,
                            'step_description_id': self.stepd1.id}]
        self.assertListEqual(result, expected_result)

    def test_get_session_step_from_name_several_steps(self):
        """Tests that getting steps info from session name and step name
        behaves as expected when several steps exist with this name
        for this session in the DB"""
        result = get_session_step_from_name(self.wfm_db_mock,
                                            self.session2.name, self.stepd2.name)
        expected_result = [{'id': self.step20.id, 'status': self.step20.status,
                            'progress': self.step20.progress,
                            'instance_name': self.step20.instance_name,
                            'jobid': self.step20.jobid, 'step_description_id': self.stepd2.id},
                           {'id': self.step21.id, 'status': self.step21.status,
                            'progress': self.step21.progress,
                            'instance_name': self.step21.instance_name,
                            'jobid': self.step21.jobid, 'step_description_id': self.stepd2.id}]
        self.assertListEqual(result, expected_result)


class TestCheckRemoveFile(unittest.TestCase):
    """Test that the function remove_file behaves as expected.
    """
    def test_remove_file_unknown(self):
        """Tests that remove_file behaves as expected for a non existing file"""
        fname = '/MY_UNKNOWN_FILE'
        result = os.path.isfile(fname)
        self.assertFalse(result)
        remove_file(fname)

    def test_remove_file_known(self):
        """Tests that remove_file behaves as expected for an existing file"""
        # Create a temporary file
        directory = tempfile.mkdtemp()
        fname = os.path.join(directory, "MY_FILE_TO_REMOVE")
        open(fname, 'w').close()
        result = os.path.isfile(fname)
        self.assertTrue(result)
        remove_file(fname)
        result = os.path.isfile(fname)
        self.assertFalse(result)
        os.rmdir(directory)


class TestCheckIsAbsPathName(unittest.TestCase):
    """ Test that the function check_isabspathname behaves as expected.
    """
    def test_check_isabspathname_len0(self):
        """Tests that check_isabspathname behaves as expected for
        a 0 length directory name"""
        directory = ""
        result = check_isabspathname(directory)
        expected_result = "is not a correct directory name"
        self.assertEqual(result, expected_result)

    def test_check_isabspathname_len1(self):
        """Tests that check_isabspathname behaves as expected for
        a 1 char length directory name"""
        directory = "/"
        result = check_isabspathname(directory)
        expected_result = "is not a correct directory name"
        self.assertEqual(result, expected_result)

    def test_check_isabspathname_not_absolute(self):
        """Tests that check_isabspathname behaves as expected for
        a directory that is not an absolute path"""
        directory = "tmp"
        result = check_isabspathname(directory)
        expected_result = "is not an absolute pathname"
        self.assertEqual(result, expected_result)

    def test_check_isabspathname_dir_ok(self):
        """Tests that check_isabspathname behaves as expected for
        an absolute pathname"""
        # Create a temporary directory
        directory = "/toto"
        result = check_isabspathname(directory)
        expected_result = ""
        self.assertEqual(result, expected_result)


class TestCheckIsAbsPathDir(unittest.TestCase):
    """ Test that the function check_isabspathdir behaves as expected.
    """
    def test_check_isabspathdir_len0(self):
        """Tests that check_isabspathdir behaves as expected for
        a 0 length directory name"""
        directory = ""
        result = check_isabspathdir(directory)
        expected_result = "is not a correct directory name"
        self.assertEqual(result, expected_result)

    def test_check_isabspathdir_len1(self):
        """Tests that check_isabspathdir behaves as expected for
        a 1 char length directory name"""
        directory = "/"
        result = check_isabspathdir(directory)
        expected_result = "is not a correct directory name"
        self.assertEqual(result, expected_result)

    def test_check_isabspathdir_not_absolute(self):
        """Tests that check_isabspathdir behaves as expected for
        a directory that is not an absolute path"""
        directory = "tmp"
        result = check_isabspathdir(directory)
        expected_result = "is not an absolute pathname"
        self.assertEqual(result, expected_result)

    def test_check_isabspathdir_not_existing(self):
        """Tests that check_isabspathdir behaves as expected for
        a directory that does not exist"""
        directory = "/UNKNOWN"
        result = check_isabspathdir(directory)
        expected_result = "is not a directory or does not exist"
        self.assertEqual(result, expected_result)

    def test_check_isabspathdir_file(self):
        """Tests that check_isabspathdir behaves as expected for
        a directory that is a file name"""
        # Create a temporary file
        tmpfp = tempfile.NamedTemporaryFile()
        tmpfp.write(b"This is my temporary file")
        directory = tmpfp.name
        self.assertEqual(os.path.exists(directory), True)
        result = check_isabspathdir(directory)
        # Closing it automatically deletes the temporary file
        tmpfp.close()
        expected_result = "is not a directory or does not exist"
        self.assertEqual(result, expected_result)

    def test_check_isabspathdir_dir_not_readable(self):
        """Tests that check_isabspathdir behaves as expected for
        a directory that is not readable"""
        # Create a temporary directory
        directory = tempfile.mkdtemp()
        os.chmod(directory, 0o300)
        result = check_isabspathdir(directory)
        os.rmdir(directory)
        expected_result = "cannot be accessed"
        self.assertEqual(result, expected_result)

    def test_check_isabspathdir_dir_not_writable(self):
        """Tests that check_isabspathdir behaves as expected for
        a directory that is not writable"""
        # Create a temporary directory
        directory = tempfile.mkdtemp()
        os.chmod(directory, 0o500)
        result = check_isabspathdir(directory)
        os.rmdir(directory)
        expected_result = "cannot be accessed"
        self.assertEqual(result, expected_result)

    def test_check_isabspathdir_dir_not_executable(self):
        """Tests that check_isabspathdir behaves as expected for
        a directory that is not executable"""
        # Create a temporary directory
        directory = tempfile.mkdtemp()
        os.chmod(directory, 0o600)
        result = check_isabspathdir(directory)
        os.rmdir(directory)
        expected_result = "cannot be accessed"
        self.assertEqual(result, expected_result)

    def test_check_isabspathdir_dir_ok(self):
        """Tests that check_isabspathdir behaves as expected for
        a directory that fulfills all conditions"""
        # Create a temporary directory
        directory = tempfile.mkdtemp()
        os.chmod(directory, 0o700)
        result = check_isabspathdir(directory)
        os.rmdir(directory)
        expected_result = ""
        self.assertEqual(result, expected_result)


class TestGetNewestFile(unittest.TestCase):
    """ Test that the function get_newest_file behaves as expected.
    """
    def test_get_newest_file_empty_dir(self):
        """Tests that get_newest_file behaves as expected for
        an empty directory"""
        # Create a temporary directory
        directory = tempfile.mkdtemp()
        os.chmod(directory, 0o700)
        result = get_newest_file(directory)
        os.rmdir(directory)
        expected_result = ""
        self.assertEqual(result, expected_result)

    def test_get_newest_file_dir_not_readable(self):
        """Tests that get_newest_file behaves as expected for
        a directory that is not readable"""
        # Create a temporary directory and some files in it
        directory = tempfile.mkdtemp()
        os.chmod(directory, 0o700)
        oldest = tempfile.mkstemp(dir=directory)
        time.sleep(2)
        middle = tempfile.mkstemp(dir=directory)
        time.sleep(2)
        newest = tempfile.mkstemp(dir=directory)
        os.chmod(directory, 0o200)
        result = get_newest_file(directory)
        os.chmod(directory, 0o700)
        rmtree(directory, ignore_errors=True)
        expected_result = ""
        self.assertEqual(result, expected_result)

    def test_get_newest_file_dir_not_empty(self):
        """Tests that get_newest_file behaves as expected for
        a directory that is not empty"""
        # Create a temporary directory and some files in it
        directory = tempfile.mkdtemp()
        os.chmod(directory, 0o700)
        oldest = tempfile.mkstemp(dir=directory)
        time.sleep(2)
        middle = tempfile.mkstemp(dir=directory)
        time.sleep(2)
        newest = tempfile.mkstemp(dir=directory)
        result = get_newest_file(directory)
        rmtree(directory, ignore_errors=True)
        expected_result = newest[1]
        self.assertEqual(result, expected_result)


class TestIsHestiaPath(unittest.TestCase):
    """ Test that the function is_hestia_path behaves as expected.
    """
    def test_is_hestia_path_empty_str(self):
        """Tests that is_hestia_path behaves as expected for
        an empty input string"""
        res_bool, res_str = is_hestia_path('')
        self.assertEqual(res_bool, False)

    def test_is_hestia_path_no_hestia_prefix(self):
        """Tests that is_hestia_path behaves as expected for
        an input string with a prefix != HESTIA@"""
        res_bool, res_str = is_hestia_path('XXX@YYY')
        self.assertEqual(res_bool, False)

    def test_is_hestia_path_abs_path(self):
        """Tests that is_hestia_path behaves as expected for
        an input string with a prefix = HESTIA@"""
        res_bool, res_str = is_hestia_path('HESTIA@YYY')
        self.assertEqual(res_bool, True)
        self.assertEqual(res_str, 'YYY')


class TestCheckIsSize(unittest.TestCase):
    """ Test that the function check_issize behaves as expected.
    """
    def test_check_issize_len0(self):
        """Tests that check_issize behaves as expected for
        a 0 length string"""
        size = ""
        result = check_issize(size)
        expected_result = "is not a correct size format"
        self.assertEqual(result, expected_result)

    def test_check_issize_no_num(self):
        """Tests that check_issize behaves as expected for
        a string that does not begin with a number"""
        size = "G123"
        result = check_issize(size)
        expected_result = "is not a correct size format"
        self.assertEqual(result, expected_result)

    def test_check_issize_incorrect_unit1(self):
        """Tests that check_issize behaves as expected for
        a string that contains an incorrect unit on one char"""
        size = "123A"
        result = check_issize(size)
        expected_result = "is not a correct size format"
        self.assertEqual(result, expected_result)

    def test_check_issize_incorrect_unit2(self):
        """Tests that check_issize behaves as expected for
        a string that contains an incorrect unit on 2 chars"""
        size = "123Kb"
        result = check_issize(size)
        expected_result = "is not a correct size format"
        self.assertEqual(result, expected_result)

    def test_check_issize_no_unit(self):
        """Tests that check_issize behaves as expected for
        a string that contains only a number"""
        size = "123"
        result = check_issize(size)
        expected_result = ""
        self.assertEqual(result, expected_result)

    def test_check_issize_unit_K(self):
        """Tests that check_issize behaves as expected for
        a string that contains a correct unit on one char (K)"""
        size = "123K"
        result = check_issize(size)
        expected_result = ""
        self.assertEqual(result, expected_result)

    def test_check_issize_unit_M(self):
        """Tests that check_issize behaves as expected for
        a string that contains a correct unit on one char (M)"""
        size = "123M"
        result = check_issize(size)
        expected_result = ""
        self.assertEqual(result, expected_result)

    def test_check_issize_unit_G(self):
        """Tests that check_issize behaves as expected for
        a string that contains a correct unit on one char (G)"""
        size = "123G"
        result = check_issize(size)
        expected_result = ""
        self.assertEqual(result, expected_result)

    def test_check_issize_unit_T(self):
        """Tests that check_issize behaves as expected for
        a string that contains a correct unit on one char (T)"""
        size = "123T"
        result = check_issize(size)
        expected_result = ""
        self.assertEqual(result, expected_result)

    def test_check_issize_unit_Ki(self):
        """Tests that check_issize behaves as expected for
        a string that contains a correct unit on one char (Ki)"""
        size = "123Ki"
        result = check_issize(size)
        expected_result = ""
        self.assertEqual(result, expected_result)

    def test_check_issize_unit_Mi(self):
        """Tests that check_issize behaves as expected for
        a string that contains a correct unit on one char (Mi)"""
        size = "123Mi"
        result = check_issize(size)
        expected_result = ""
        self.assertEqual(result, expected_result)

    def test_check_issize_unit_Gi(self):
        """Tests that check_issize behaves as expected for
        a string that contains a correct unit on one char (Gi)"""
        size = "123Gi"
        result = check_issize(size)
        expected_result = ""
        self.assertEqual(result, expected_result)

    def test_check_issize_unit_Gi(self):
        """Tests that check_issize behaves as expected for
        a string that contains a correct unit on one char (Gi)"""
        size = "123GiB"
        result = check_issize(size)
        expected_result = ""
        self.assertEqual(result, expected_result)

    def test_check_issize_unit_Ti(self):
        """Tests that check_issize behaves as expected for
        a string that contains a correct unit on one char (Ti)"""
        size = "123Ti"
        result = check_issize(size)
        expected_result = ""
        self.assertEqual(result, expected_result)

    def test_check_issize_unit1_space(self):
        """Tests that check_issize behaves as expected for
        a string that contains a correct unit on one char separated by a space"""
        size = "123 T"
        result = check_issize(size)
        expected_result = "is not a correct size format"
        self.assertEqual(result, expected_result)

    def test_check_issize_unit2_space(self):
        """Tests that check_issize behaves as expected for
        a string that contains a correct unit on 2 chars separated by a space"""
        size = "123 Ab"
        result = check_issize(size)
        expected_result = "is not a correct size format"
        self.assertEqual(result, expected_result)


class TestSetupSessionFields(unittest.TestCase):
    """ Test that the function setup_session_fields behaves as expected.
    """
    def test_setup_session_fields_ok(self):
        """Tests that setup_session_fields behaves as expected.
        """
        result = setup_session_fields('w1', 'n1', 's1')
        expected_result =  { 'workflow_name': 'w1', 'name': 'n1', 'status': 's1', 'steps': [] }
        self.assertEqual(result, expected_result)


class TestSetupServiceFields(unittest.TestCase):
    """ Test that the function setup_service_fields behaves as expected.
    """
    def test_setup_service_fields_no_service(self):
        """Tests that setup_service_fields behaves as expected when
        the list of services is empty
        """
        services = []
        result = setup_service_fields(services)
        expected_result = {
            'name': 'UNKNOWN',
            'type': 'UNKNOWN',
            'status': 'UNKNOWN',
            'jobid': 0
        }
        self.assertEqual(result, expected_result)

    def test_setup_service_fields_single_service(self):
        """Tests that setup_service_fields behaves as expected when
        the list of services contains a single element
        """
        services = [ {
            'name': 'name0',
            'type': 'TYPE0',
            'status': 'status0',
            'jobid': 123
        } ]
        result = setup_service_fields(services)
        expected_result = {
            'name': 'name0',
            'type': 'TYPE0',
            'status': 'status0',
            'jobid': 123
        }
        self.assertEqual(result, expected_result)

    def test_setup_service_fields_two_services(self):
        """Tests that setup_service_fields behaves as expected when
        the list of services contains 2 elements
        """
        services = [ {
            'name': 'name0',
            'type': 'TYPE0',
            'status': 'status0',
            'jobid': 123
        }, {
            'name': 'name1',
            'type': 'TYPE1',
            'status': 'status1',
            'jobid': 456
        } ]
        result = setup_service_fields(services)
        expected_result = {
            'name': 'name0',
            'type': 'TYPE0',
            'status': 'status0',
            'jobid': 123
        }
        self.assertEqual(result, expected_result)


class TestSetupStepsFields(unittest.TestCase):
    """ Test that the function setup_steps_fields behaves as expected.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        """
        test_config = Path(__file__).parent.absolute() / "test_data" / "slurm_settings.yaml"
        api_settings = WFMSettings.from_yaml(test_config)
        self.job_mgr_commands = api_settings.command
        self.job_manager = api_settings.jobmanager

    def test_setup_steps_fields_no_step(self):
        """Tests that setup_steps_fields behaves as expected when
        the list of steps is empty
        """
        stepd = {
            'id': 0,
            'session_id': 1,
            'service_id': 2,
            'name': 'name0',
            'command': 'command0'
        }
        step_list = []
        service = {
            'name': 'name1',
            'type': 'TYPE0',
            'targets': 'target0',
            'status': 'status0',
            'jobid': 123
        }
        result = setup_steps_fields(stepd, step_list, service,
                                    self.job_manager.name, self.job_mgr_commands)
        expected_result = [ {
            'name': stepd['name'],
            'status': 'INACTIVE',
            'progress': "",
            'jobid': 0,
            'command': stepd['command'],
            'service': service
        } ]
        self.assertListEqual(result, expected_result)

    def test_setup_steps_fields_two_steps(self):
        """Tests that setup_steps_fields behaves as expected when
        the list of steps contains 2 steps
        """
        stepd = {
            'id': 0,
            'session_id': 1,
            'service_id': 2,
            'name': 'name0',
            'command': 'command0'
        }
        step_list = [ {
            'id': 0,
            'instance_name': 'name0_0',
            'status': 'status0',
            'progress': 'Copying 1%',
            'jobid': 123,
            'step_description_id': 0
        }, {
            'id': 1,
            'instance_name': 'name0_1',
            'status': 'status1',
            'progress': 'Copying 2%',
            'jobid': 456,
            'step_description_id': 0
        } ]
        service = {
            'name': 'name2',
            'type': 'TYPE2',
            'targets': 'target2',
            'status': 'status2',
            'jobid': 789
        }
        result = setup_steps_fields(stepd, step_list, service,
                                    self.job_manager.name, self.job_mgr_commands)
        expected_result = [ {
            'name': step_list[0]['instance_name'],
            'status': step_list[0]['status'],
            'progress': step_list[0]['progress'],
            'jobid': step_list[0]['jobid'],
            'command': stepd['command'],
            'service': service
        }, {
            'name': step_list[1]['instance_name'],
            'status': step_list[1]['status'],
            'progress': step_list[1]['progress'],
            'jobid': step_list[1]['jobid'],
            'command': stepd['command'],
            'service': service
        } ]
        self.assertListEqual(result, expected_result)


if __name__ == "__main__":
    unittest.main()
