"""Tests WFM API endpoints.
"""

import unittest
from tests.test_app.test_endpoints import TestEntrypoints


class TestGetAllSession(TestEntrypoints):
    """Tests for the endpoints /session/all.
    """

    @unittest.skip('skipping get all sessions')
    def test_get_all_session(self):
        """Test that the correct list of sessions is returned
        """
        with self.test_app as test:
            response = test.get("/session/all")
            # Check that status code is 200
            self.assertEqual(response.status_code, 200)
            expected_list = [
            {'id': '1', 'name': 's1', 'workflow_name': 'test1',
             'start_time': 0, 'end_time': 0, 'status': "starting"}]
            self.assertListEqual(response.json(), expected_list)


class TestGetSession(TestEntrypoints):
    """Tests for the endpoints /session/{session_name}.
    """

    @unittest.skip('skipping get session')
    def test_get_session(self):
        """Test that given a session name the correct list of sessions is returned
        """
        session_name = "s1"
        with self.test_app as test:
            response = test.get(f"/session/{session_name}")
            # Check that status code is 200
            self.assertEqual(response.status_code, 200)
            expected_list = [
            {'id': '1', 'name': 's1', 'workflow_name': 'test1',
             'start_time': 0, 'end_time': 0, 'status': "starting"}]
            self.assertListEqual(response.json(), expected_list)

class TestPostSession(TestEntrypoints):
    """Tests for the endpoints /session/startup.
    """

    def test_startup_session_sbb(self):
        """Test that given a session name and an workflow path
        a new session created (service SBB)
        """
        session_name = "new_s1_sbb"
        workflow_path= self.wdf_sbb_ok

        yfile = open(workflow_path, "r", encoding="utf-8")
        wf_description = yfile.read()

        with self.test_app as test:
            response = test.post("/session/startup",
                                 json={'workflow_description_file': str(workflow_path),
                                       'workflow_description': wf_description,
                                       'synchronous': True,
                                       'session_name': str(session_name),
                                       'user_name': 'myname',
                                       'replacements': {}})
            # Check that status code is 200
            self.assertEqual(response.status_code, 200)

            # Check that the new session is in the database
            response = test.get(f"/session/{session_name}")
            # Check that status code is 200
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json()[0]['name'], session_name)
            self.assertEqual(response.json()[0]['workflow_name'], 'My_Workflow1_sbb')

    def test_startup_session_nfs(self):
        """Test that given a session name and an workflow path
        a new session created (service NFS)
        """
        session_name = "new_s1_nfs"
        workflow_path= self.wdf_nfs_ok

        yfile = open(workflow_path, "r", encoding="utf-8")
        wf_description = yfile.read()

        with self.test_app as test:
            response = test.post("/session/startup",
                                 json={'workflow_description_file': str(workflow_path),
                                       'workflow_description': wf_description,
                                       'synchronous': True,
                                       'session_name': str(session_name),
                                       'user_name': 'myname',
                                       'replacements': {}})
            # Check that status code is 200
            self.assertEqual(response.status_code, 200)

            # Check that the new session is in the database
            response = test.get(f"/session/{session_name}")
            # Check that status code is 200
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json()[0]['name'], session_name)
            self.assertEqual(response.json()[0]['workflow_name'], 'My_Workflow1_nfs')

if __name__ == "__main__":
    unittest.main()
