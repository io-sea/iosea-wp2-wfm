"""Tests that utility routines of the CLI work as expected.
"""
import unittest

from iosea_wf.utils.utils import convert_to_dict, compute_max_len, compute_second_level_max_len
from iosea_wf.utils.utils import compute_third_level_max_len
from iosea_wf.utils.errors import VariableDefinitionSyntaxError


class TestComputeMaxLen(unittest.TestCase):
    """Test that the function compute_max_len behaves as expected.
    """
    def test_compute_max_len_list_empty(self):
        """Tests that compute_max_len behaves as expected when
        empty list is provided"""
        input_list = []
        result = compute_max_len(input_list, "any_field")
        expected_result = 0
        self.assertEqual(result, expected_result)

    def test_compute_max_len_list_single_unknown_field(self):
        """Tests that compute_max_len behaves as expected when
        list with single element is provided"""
        input_list = [ { "field1": "value1", "field2": "value2"} ]
        result = compute_max_len(input_list, "field1")
        expected_result = len(input_list[0]["field1"])
        self.assertEqual(result, expected_result)
        result = compute_max_len(input_list, "field2")
        expected_result = len(input_list[0]["field2"])
        self.assertEqual(result, expected_result)
        result = compute_max_len(input_list, "unknown")
        expected_result = 0
        self.assertEqual(result, expected_result)

    def test_compute_max_len_list_several_unknown_fields(self):
        """Tests that compute_max_len behaves as expected when
        list with several elements is provided"""
        input_list = [ { "field1": "value1",
                         "field2": "elem_0_is_the_longest_value2",
                         "field3": "value3"},
                       { "field1": "elem_1_is_the_longest_value1",
                         "field2": "value2"} ]
        result = compute_max_len(input_list, "field1")
        expected_result = len(input_list[1]["field1"])
        self.assertEqual(result, expected_result)
        result = compute_max_len(input_list, "field2")
        expected_result = len(input_list[0]["field2"])
        self.assertEqual(result, expected_result)
        result = compute_max_len(input_list, "field3")
        expected_result = len(input_list[0]["field3"])
        self.assertEqual(result, expected_result)
        result = compute_max_len(input_list, "unknown")
        expected_result = 0
        self.assertEqual(result, expected_result)


class TestComputeSecondLevelMaxLen(unittest.TestCase):
    """Test that the function compute_second_level_max_len behaves as expected.
    """
    def setUp(self):
        self.longest_str_field = 'loooooooooooooooooooooooooooooooooooooongest field'
        self.extra_field = 'extra_field'
        self.extra_field_value = self.longest_str_field
        self.longest_int_field = 1234567890
        self.input_list = [ {
            'workflow_name': 'W1', 'name': 'session1', 'status': 'session_status1',
            'steps': [{
                'name': 'step_name1_1', 'status': 'step_status1_1', 'jobid': None,
                'command': 'command1_1',
                'extra_field': self.extra_field_value,
                'service': {'name': 'service_name1_1', 'type': 'type1_1', 'targets': 'targets1_1',
                            'status': 'service_status1_1', 'jobid': 1}
            }, {
                'name': 'step_name1_2', 'status': 'step_status1_2', 'jobid': 123,
                'command': 'command1_2',
                'service': {'name': 'service_name1_2', 'type': 'type1_2', 'targets': 'targets1_2',
                            'status': 'service_status1_2', 'jobid': 2}
            }]
        }, {
            'workflow_name': 'W2', 'name': 'session2', 'status': 'session_status2',
            'steps': [{
                'name': 'step_name2', 'status': self.longest_str_field, 'jobid': None,
                'command': 'command2',
                'service': {'name': 'service_name2', 'type': 'type2', 'targets': 'targets2',
                            'status': 'service_status2', 'jobid': 3}
            }]
        }, {
            'workflow_name': 'W3', 'name': 'session3', 'status': 'session_status3',
            'steps': [{
                'name': 'step_name3_1', 'status': 'step_status3_1', 'jobid': self.longest_int_field,
                'command': 'command3_1',
                'service': {'name': 'service_name3_1', 'type': 'type3_1', 'targets': 'targets3_1',
                            'status': 'service_status3_1', 'jobid': 1321}
            }, {
                'name': 'step_name3_2', 'status': 'step_status3_2', 'jobid': 456789, 
                'command': 'command3_2',
                'service': {'name': 'service_name3_2', 'type': 'type3_2', 'targets': 'targets3_2',
                            'status': 'service_status3_2', 'jobid': 61321}
            }]
        }]

    def test_compute_second_level_max_len_list_empty_0(self):
        """Tests that compute_second_level_max_len behaves as expected when
        empty list is provided"""
        initial_len = 0
        input_list = []
        result = compute_second_level_max_len(initial_len, input_list, "any_subdict_field",
                                              "any_field")
        expected_result = initial_len
        self.assertEqual(result, expected_result)

    def test_compute_second_level_max_len_list_empty_100(self):
        """Tests that compute_second_level_max_len behaves as expected when
        empty list is provided"""
        initial_len = 100
        input_list = []
        result = compute_second_level_max_len(initial_len, input_list, "any_subdict_field",
                                              "any_field")
        expected_result = initial_len
        self.assertEqual(result, expected_result)

    def test_compute_second_level_max_len_unknown_fields(self):
        """Tests that compute_second_level_max_len behaves as expected when
        unknonw fields are provided"""
        initial_len = 0
        result = compute_second_level_max_len(initial_len, self.input_list, "steps",
                                              self.extra_field)
        expected_result = len(self.extra_field_value)
        self.assertEqual(result, expected_result)
        result = compute_second_level_max_len(initial_len, self.input_list, "steps", "unknown")
        expected_result = 0
        self.assertEqual(result, expected_result)
        result = compute_second_level_max_len(initial_len, self.input_list, "steps", "jobid")
        expected_result = len(str(self.longest_int_field))
        self.assertEqual(result, expected_result)

    def test_compute_second_level_max_len_all_fields_known(self):
        """Tests that compute_second_level_max_len behaves as expected when
        all provided fields are known"""
        result = compute_second_level_max_len(len('STATUS'), self.input_list, "steps", "status")
        expected_result = len(self.longest_str_field)
        self.assertEqual(result, expected_result)


class TestComputeThirdLevelMaxLen(unittest.TestCase):
    """Test that the function compute_third_level_max_len behaves as expected.
    """
    def setUp(self):
        self.longest_str_field = 'loooooooooooooooooooooooooooooooooooooongest field'
        self.extra_field1 = 'extra_field1'
        self.extra_field2 = 'extra_field2'
        self.extra_field_value = self.longest_str_field
        self.longest_int_field = 1234567890
        self.input_list = [ {
            'workflow_name': 'W1', 'name': 'session1', 'status': 'session_status1',
            'steps': [{
                'name': 'step_name1_1', 'status': 'step_status1_1', 'jobid': None,
                'command': 'command1_1',
                'service': {'name': 'service_name1_1', 'type': 'type1_1', 'targets': 'targets1_1',
                            'status': 'service_status1_1', 'jobid': None}
            }, {
                'name': 'step_name1_2', 'status': 'step_status1_2', 'jobid': 123,
                'command': 'command1_2',
                'service': {'name': 'service_name1_2', 'type': 'type1_2', 'targets': 'targets1_2',
                            'status': 'service_status1_2', 'jobid': 2}
            }]
        }, {
            'workflow_name': 'W2', 'name': 'session2', 'status': 'session_status2',
            'steps': [{
                'name': 'step_name2', 'status': 'session_status2', 'jobid': None,
                'command': 'command2',
                'service': {'name': 'service_name2', 'type': 'type2', 'targets': 'targets2',
                            'extra_field2': self.extra_field_value,
                            'status': 'service_status2', 'jobid': self.longest_int_field}
            }]
        }, {
            'workflow_name': 'W3', 'name': 'session3', 'status': 'session_status3',
            'steps': [{
                'name': 'step_name3_1', 'status': 'step_status3_1', 'jobid': 456,
                'command': 'command3_1',
                'service': {'name': 'service_name3_1', 'type': 'type3_1', 'targets': 'targets3_1',
                            'status': 'service_status3_1', 'jobid': 1321}
            }, {
                'name': 'step_name3_2', 'status': 'step_status3_2', 'jobid': 456789, 
                'command': 'command3_2',
                'service': {'name': 'service_name3_2', 'type': self.longest_str_field, 'targets': 'targets3_2',
                            'status': 'service_status3_2', 'jobid': 61321}
            }]
        }]

    def test_compute_third_level_max_len_list_empty_0(self):
        """Tests that compute_third_level_max_len behaves as expected when
        empty list is provided"""
        initial_len = 0
        input_list = []
        result = compute_third_level_max_len(initial_len, input_list, "any_subdict_field1",
                                             "any_subdict_field2", "any_field")
        expected_result = initial_len
        self.assertEqual(result, expected_result)

    def test_compute_third_level_max_len_list_empty_100(self):
        """Tests that compute_third_level_max_len behaves as expected when
        empty list is provided"""
        initial_len = 100
        input_list = []
        result = compute_third_level_max_len(initial_len, input_list, "any_subdict_field1",
                                             "any_subdict_field2", "any_field")
        expected_result = initial_len
        self.assertEqual(result, expected_result)

    def test_compute_third_level_max_len_unknown_fields(self):
        """Tests that compute_third_level_max_len behaves as expected when
        unknonw fields are provided"""
        initial_len = 0
        result = compute_third_level_max_len(initial_len, self.input_list, "steps",
                                             'service', self.extra_field2)
        expected_result = len(self.extra_field_value)
        self.assertEqual(result, expected_result)
        result = compute_third_level_max_len(initial_len, self.input_list, "steps",
                                             "service", "unknown")
        expected_result = 0
        self.assertEqual(result, expected_result)
        result = compute_third_level_max_len(initial_len, self.input_list, "steps",
                                             "service", "jobid")
        expected_result = len(str(self.longest_int_field))
        self.assertEqual(result, expected_result)

    def test_compute_third_level_max_len_all_fields_known(self):
        """Tests that compute_third_level_max_len behaves as expected when
        all provided fields are known"""
        result = compute_third_level_max_len(len('STATUS'), self.input_list, "steps",
                                             "service", "type")
        expected_result = len(self.longest_str_field)
        self.assertEqual(result, expected_result)


class TestConvertToDict(unittest.TestCase):
    """Test that the function convert_to_dict behaves as expected.
    """
    def test_convert_to_dict_empty_def(self):
        """Tests that convert_to_dict behaves as expected when
        empty definition string is provided"""
        definitions = []
        result = convert_to_dict(definitions)
        expected_result = {}
        self.assertDictEqual(result, expected_result)

    def test_convert_to_dict_def_no_equal_sign(self):
        """Tests that convert_to_dict behaves as expected when
        no equal sign in the definition string"""
        definitions = ['variable']
        with self.assertRaises(VariableDefinitionSyntaxError):
            convert_to_dict(definitions)

    def test_convert_to_dict_def_two_equal_signs(self):
        """Tests that convert_to_dict behaves as expected when
        2 equal signs in the definition string"""
        definitions = ['variable=value1=value2']
        with self.assertRaises(VariableDefinitionSyntaxError):
            convert_to_dict(definitions)

    def test_convert_to_dict_var1_non_alpha(self):
        """Tests that convert_to_dict behaves as expected when
        the variable name begins with a non aplhpa char in the definition string"""
        definitions = ['9variable=value']
        with self.assertRaises(VariableDefinitionSyntaxError):
            convert_to_dict(definitions)

    def test_convert_to_dict_var_body_non_alnum(self):
        """Tests that convert_to_dict behaves as expected when
        the variable name (starting from 2nd character) contains non alnum
        chars or '_' in the definition string"""
        definitions = ['variable,_zt=value']
        with self.assertRaises(VariableDefinitionSyntaxError):
            convert_to_dict(definitions)

    def test_convert_to_dict_single_definition_ok(self):
        """Tests that convert_to_dict behaves as expected when single definition
        and the variable name is OK in the definition string"""
        definitions = ['variable_1=value1']
        result = convert_to_dict(definitions)
        expected_result = {'{{ variable_1 }}': 'value1'}
        self.assertDictEqual(result, expected_result)


if __name__ == "__main__":
    unittest.main()
