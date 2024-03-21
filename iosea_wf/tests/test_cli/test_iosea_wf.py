"""Tests for the CLI part of the Workflow Manager.
"""
import unittest
from getpass import getuser
import os
import re
from itertools import count
from click.testing import CliRunner
from tests.test_cli import TestEntrypoints
from iosea_wf.iosea_wf import cli

__copyright__ = """
Copyright (C) Bull S. A. S.
"""
# username
USERNAME = getuser()
# session unique index by test
index = count()

class TestCliStartStop(TestEntrypoints):
    """ Test that the start and stop actions of the CLI behaves as expected.
    """
    def test_start_unexistent_file(self):
        """Tests that calling the start command with a non existing WDF
        works as expected"""
        unexistent_file = "/tmp/unexistent_file"
        runner = CliRunner()
        result = runner.invoke(cli, ['start', '-w', unexistent_file, '-s', 'session0'])
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn('Error: ', result.output)
        expected_error = f"Path \'{unexistent_file}\' does not exist"
        self.assertIn(expected_error, result.output)

    def test_start_wrong_file(self):
        """Tests that calling the start command with a WDF that is not syntactically correct
        works as expected"""
        input_file = self.wrong_wdf
        runner = CliRunner()
        result = runner.invoke(cli, ['start', '-w', input_file, '-s', 'session0'])
        self.assertEqual(result.exit_code, 1)
        expected_error = f"File {input_file} is not syntactically correct"
        self.assertIn(expected_error, result.output)

    def test_start_and_stop_ok(self):
        """Tests that calling the start and stop commands with correct parameters
        works as expected"""
        session = f'session{next(index)}'
        runner = CliRunner()
        result = runner.invoke(cli, ['start', '-w', self.wdf_sbb, '-s',
                                     session, '--syncstart', '--settings', self.settings])
        self.assertEqual(result.exit_code, 0)
        expected_output = f"Successfully started session {session}\n"
        self.assertEqual(expected_output, result.output)
        result = runner.invoke(cli, ['stop', '-s', session, '--syncstop',
                                     '--settings', self.settings])
        self.assertEqual(result.exit_code, 0)
        expected_output = f"Successfully stopped session {session}\n"
        self.assertEqual(expected_output, result.output)

    def test_stop_ko(self):
        """Tests that calling the stop command with a session that did not start
        works as expected"""
        # This test will stop the sessionX which does not exist
        session = 'sessionX'
        runner = CliRunner()
        result = runner.invoke(cli, ['stop', '-s', session, '--syncstop',
                                     '--settings', self.settings])
        self.assertEqual(result.exit_code, 1)
        self.assertRegex(result.output, r'.*No session with name sessionX.*')

    def test_stderr_info(self):
        """Tests settings file to display logs on stderr"""
        # This test will stop the sessionX which does not exist
        session = 'sessionX'
        runner = CliRunner()
        settings = os.path.join(self.config_dir, 'cli_settings_debug.yaml')
        result = runner.invoke(cli, ['stop', '-s', session, '--syncstop', '--settings', settings])
        self.assertEqual(result.exit_code, 1)
        self.assertRegex(result.output, r'.*DEBUG.*post\(http://0.0.0.0:.*/session/stop,' +
                         r' sync_stop=True, session_name=sessionX\).*')

class TestCliRun(TestEntrypoints):
    """ Test that the run action of the CLI behaves as expected.
    """
    def test_run_step(self):
        """Tests that run step works as expected with a started session
        and then stopped session and test again"""
        session = f'session{next(index)}'
        wdf = self.wdf_sbb
        cli_arguments = ['-s', session, '--settings', self.settings]
        runner = CliRunner()
        result = runner.invoke(cli, ['start', '-w', wdf, '--syncstart'] + cli_arguments)
        self.assertEqual(result.exit_code, 0)
        result = runner.invoke(cli, ['run', "--step", "step_A1"] + cli_arguments)
        self.assertEqual(result.exit_code, 0)
        step_instance = f"{USERNAME}-{session}-step_A1_1"
        expected_output = f"Successfully submitted step_A1 step: {step_instance}\n"
        self.assertEqual(result.output, expected_output)
        result = runner.invoke(cli, ['stop', '--syncstop', '--force'] + cli_arguments)
        self.assertEqual(result.exit_code, 0)

    def test_run_step_twice(self):
        """Tests that run the same step twice works as expected
        """
        session = f'session{next(index)}'
        wdf = self.wdf_sbb
        stepd = "step_A1"
        cli_arguments = ['-s', session, '--settings', self.settings]
        runner = CliRunner()
        result = runner.invoke(cli, ['start', '-w', wdf, '--syncstart'] + cli_arguments)
        self.assertEqual(result.exit_code, 0)
        result = runner.invoke(cli, ['run', "--step", f"{stepd}"] + cli_arguments)
        self.assertEqual(result.exit_code, 0)
        step_instance = f"{USERNAME}-{session}-{stepd}_1"
        expected_output = f"Successfully submitted {stepd} step: {step_instance}\n"
        self.assertEqual(expected_output, result.output)
        result = runner.invoke(cli, ['run', "--step", f"{stepd}"] + cli_arguments)
        self.assertEqual(result.exit_code, 0)
        step_instance = f"{USERNAME}-{session}-{stepd}_2"
        expected_output = f"Successfully submitted {stepd} step: {step_instance}\n"
        self.assertEqual(expected_output, result.output)
        result = runner.invoke(cli, ['stop', '--syncstop', '--force'] + cli_arguments)
        self.assertEqual(result.exit_code, 0)

class TestCliStatus(TestEntrypoints):
    """ Test that the status action of the CLI behaves as expected.
    """
    def test_status_session(self):
        """Tests that status session works as expected with a started session
        and then with a stopped session"""
        session = f'session{next(index)}'
        wdf = self.wdf_sbb
        cli_arguments = ['-s', session, '--settings', self.settings]
        runner = CliRunner()
        result = runner.invoke(cli, ['start', '-w', wdf, '--syncstart'] + cli_arguments)
        self.assertEqual(result.exit_code, 0)
        result = runner.invoke(cli, ['status'] + cli_arguments)
        self.assertEqual(result.exit_code, 0)
        self.assertRegex(result.output, r'.*' + re.escape(session) + '.*active.*')
        result = runner.invoke(cli, ['status',  '-a', '--settings', self.settings])
        self.assertEqual(result.exit_code, 0)
        self.assertRegex(result.output, r'.*' + re.escape(session) + '.*active.*')
        result = runner.invoke(cli, ['stop', '--syncstop', '--force'] + cli_arguments)
        self.assertEqual(result.exit_code, 0)
        result = runner.invoke(cli, ['status'] + cli_arguments)
        self.assertEqual(result.exit_code, 0)
        expected_output = f"No session with name {session}"
        self.assertTrue(expected_output in result.output)

    def test_status_service(self):
        """Tests that status service works as expected with a started session
        and then with a stopped session"""
        session = f'session{next(index)}'
        wdf = self.wdf_sbb
        cli_arguments = ['-s', session, '--settings', self.settings]
        runner = CliRunner()
        result = runner.invoke(cli, ['start', '-w', wdf, '--syncstart'] + cli_arguments)
        self.assertEqual(result.exit_code, 0)
        service_name = f"{USERNAME}-{session}-lqcd-sbb1"
        result = runner.invoke(cli, ['status', '-S', service_name, '--settings', self.settings])
        self.assertEqual(result.exit_code, 0)
        self.assertRegex(result.output,
                         r'.*' + re.escape(service_name) + r'.*[stopped|allocated].*')
        result = runner.invoke(cli, ['status',  '-A', '--settings', self.settings])
        self.assertEqual(result.exit_code, 0)
        self.assertRegex(result.output,
                         r'.*' + re.escape(service_name) + r'.*[stopped|allocated].*')
        result = runner.invoke(cli, ['stop', '--syncstop', '--force'] + cli_arguments)
        self.assertEqual(result.exit_code, 0)
        result = runner.invoke(cli, ['status', '-A', '--settings', self.settings])
        self.assertEqual(result.exit_code, 0)
        expected_output = "No service found in the WFDB"
        self.assertTrue(expected_output in result.output)

    def test_status_step(self):
        """Tests that status step works as expected with existing step instance
        and then with an unexistent step instance"""
        session = f'session{next(index)}'
        wdf = self.wdf_sbb
        cli_arguments = ['-s', session, '--settings', self.settings]
        runner = CliRunner()
        result = runner.invoke(cli, ['start', '-w', wdf, '--syncstart'] + cli_arguments)
        self.assertEqual(result.exit_code, 0)
        # run the step_A1 first time
        result = runner.invoke(cli, ['run', "--step", "step_A1"] + cli_arguments)
        self.assertEqual(result.exit_code, 0)
        result = runner.invoke(cli, ['status', "--step", "step_A1"] + cli_arguments)
        self.assertEqual(result.exit_code, 0)
        self.assertRegex(result.output, r'.*step_A1_1.*[stopped|running].*')
        # run the step_A1 second time
        result = runner.invoke(cli, ['run', "--step", "step_A1"] + cli_arguments)
        self.assertEqual(result.exit_code, 0)
        result = runner.invoke(cli, ['status', "-T"] + cli_arguments)
        self.assertEqual(result.exit_code, 0)
        self.assertRegex(result.output, r'.*step_A1_1.*[stopped|running].*')
        self.assertRegex(result.output, r'.*step_A1_2.*[stopped|running].*')
        result = runner.invoke(cli, ['status', "--step", "step_A0"] + cli_arguments)
        self.assertEqual(result.exit_code, 0)
        expected_output_re = (r'.*Step step_A0 not stored in the WFM DB for session ' +
                             re.escape(session) + r'.*')
        self.assertRegex(result.output, expected_output_re)
        result = runner.invoke(cli, ['stop', '--syncstop', '--force'] + cli_arguments)
        self.assertEqual(result.exit_code, 0)
        result = runner.invoke(cli, ['status', "--step", "step_A1"] + cli_arguments)
        self.assertEqual(result.exit_code, 0)
        expected_output_re = r'.*Session ' + re.escape(session) + ' not stored in the WFM DB.*'
        self.assertRegex(result.output, expected_output_re)

if __name__ == "__main__":
    unittest.main()
