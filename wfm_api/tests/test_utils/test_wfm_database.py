"""Unit tests for the WFM database methodes.
"""

from sqlalchemy.sql import text

import unittest
from tests.test_utils import TEST_DATABASE
from wfm_api.utils.database.wfm_database import WFMDatabase
from wfm_api.utils.database.wfm_database import Session, Service, Base
from wfm_api.utils.database.wfm_database import Step, StepDescription
from wfm_api.utils.database.wfm_database import ObjectActivityLogging
from wfm_api.utils.database.wfm_database import NamespaceLock
from wfm_api.utils.errors import UnexistingSessionNameError
from wfm_api.utils.errors import UnexistingServiceNameError, NoDocumentError, NoUniqueDocumentError


class TestWFMDatabase(unittest.TestCase):
    """Tests that the manipulation of the sqlite database behaves as expected.
    """

    def setUp(self):
        """Connect to database for tests.
        """
        self.db_test = WFMDatabase(name=TEST_DATABASE)

    def tearDown(self):
        """Close database after tests.
        """
        self.db_test.dbsession.close()

    def test_get_session_info_from_name_no_workflow(self):
        """Tests that getting all the rows from a Session table
           with a specific session name and no workflow name behaves as expected
        """
        session_name = 's1'
        expected_list = [
            {'id': 1, 'workflow_name': 'test1', 'name': 's1',
             'start_time': 0, 'end_time': 0, 'status': 'starting'}]
        result = self.db_test.get_session_info_from_name(sname=session_name)
        self.assertListEqual(result, expected_list)

    def test_get_session_info_from_name_workflow(self):
        """Tests that getting all the rows from a Session table
           with specific session and workflow names behaves as expected
        """
        session_name = 's1'
        workflow_name = 'test1'
        expected_list = [
            {'id': 1, 'workflow_name': workflow_name, 'name': session_name,
             'start_time': 0, 'end_time': 0, 'status': 'starting'}]
        result = self.db_test.get_session_info_from_name(sname=session_name,
                                                         wname=workflow_name)
        self.assertListEqual(result, expected_list)

    def test_get_session_info_from_name_unexistent_user(self):
        """Tests that getting all the rows from a Session table
           with a specific session name and no name behaves as expected
        """
        session_name = 's1'
        with self.assertRaises(UnexistingSessionNameError):
            self.db_test.get_session_info_from_name(sname=session_name,
                                                    uname='unexistent_user')

    def test_get_all_services(self):
        """Tests that getting all the rows from a Service table behaves as expected"""
        expected_list = [
            {'id': 1, 'session_id': 1, 'name': 'e1', 'type': 'SBB', 'location': 'location1',
             'targets': '/target1', 'status': 'status1', 'jobid': 1},
            {'id': 2, 'session_id': 2, 'name': 'e2', 'type': 'SBB', 'location': 'location2',
             'targets': '/target2', 'status': 'status2', 'jobid': 2}]
        result = self.db_test.get_all_services()
        self.assertListEqual(result, expected_list)

    def test_get_service_info_from_name(self):
        """Tests that getting all the rows from a Service table
           with a specific name behaves as expected
        """
        service_name = 'e1'
        expected_list = [
            {'id': 1, 'session_id': 1, 'name': 'e1', 'type': 'SBB', 'location': 'location1',
             'targets': '/target1', 'status': 'status1', 'jobid': 1}]
        result = self.db_test.get_service_info_from_name(service_name)
        self.assertListEqual(result, expected_list)


class TestAddUniqueSession(unittest.TestCase):
    """ Test that the function add_unique_session behaves as expected.
    """
    def setUp(self):
        """Connect to database for tests.
        """
        self.db_test = WFMDatabase(name=TEST_DATABASE)

        # ids of the already stored sessions (see __init__.py)
        self.ids = [ 1, 2 ]

    def tearDown(self):
        self.db_test.dbsession.close()

    def test_add_unique_session_unexisting_session(self):
        """Tests that adding a not yet existing session behaves as expected"""
        result_add = self.db_test.add_unique_session('unexisting_name',
                                                     'unexisting_wfname',
                                                     'user',
                                                     123, 'starting')
        self.assertNotIn(result_add, self.ids)
        # Check that we generated a log into the DB
        query = f"object_type == 'session' AND object_id == {result_add}"
        result_query = self.db_test.dbsession.query(ObjectActivityLogging).filter(text(query)).all()
        list_dicts = [item.dict() for item in result_query]
        self.assertEqual(len(list_dicts), 1)
        expected_result = {'id': 1, 'object_type': 'session',
                            'object_id': result_add, 'activity': 'creation'}
        # we do this instead of assertListEqual because the id might not be '1' since we already
        # added some logs in the init
        self.assertEqual(list_dicts[0]['object_type'], expected_result['object_type'])
        self.assertEqual(list_dicts[0]['object_id'], expected_result['object_id'])
        self.assertEqual(list_dicts[0]['activity'], expected_result['activity'])

    def test_add_unique_session_existing_session(self):
        """Tests that adding an already existing session behaves as expected"""
        result = self.db_test.add_unique_session('s1', 'test1', 'user', 0, 'starting')
        self.assertEqual(result, 1)


class TestAddSession(unittest.TestCase):
    """ Test that the function add_session behaves as expected.
    """
    def setUp(self):
        """Connect to database for tests.
        """
        self.db_test = WFMDatabase(name=TEST_DATABASE)

        # ids of the already stored sessions (see __init__.py)
        self.ids = [ 1, 2 ]
        self.unexisting_wfname = 'unexisting_wfname'

    def tearDown(self):
        self.db_test.dbsession.close()

    def test_add_session_unexisting_session(self):
        """Tests that adding a not yet existing session behaves as expected"""
        result_add = self.db_test.add_session('unexisting_name', self.unexisting_wfname, 'user',
                                              123, 'starting')
        self.assertNotIn(result_add, self.ids)
        # Check that we generated a log into the DB
        query = f"object_type == 'session' AND object_id == {result_add}"
        result_query = self.db_test.dbsession.query(ObjectActivityLogging).filter(text(query)).all()
        list_dicts = [item.dict() for item in result_query]
        self.assertEqual(len(list_dicts), 1)
        expected_result = {'id': 1, 'object_type': 'session',
                            'object_id': result_add, 'activity': 'creation'}
        # we do this instead of assertListEqual because the id might not be '1' since we already
        # added some logs in the init
        self.assertEqual(list_dicts[0]['object_type'], expected_result['object_type'])
        self.assertEqual(list_dicts[0]['object_id'], expected_result['object_id'])
        self.assertEqual(list_dicts[0]['activity'], expected_result['activity'])

    def test_add_session_existing_session(self):
        """Tests that adding an already existing session behaves as expected"""
        result1 = self.db_test.add_session('unexisting_name', self.unexisting_wfname, 'user',
                                           123, 'starting')
        self.assertNotIn(result1, self.ids)
        # Add it a 2nd time, it should be added with a different id
        result2 = self.db_test.add_session('unexisting_name', self.unexisting_wfname, 'user',
                                           123, 'starting')
        self.assertNotEqual(result2, result1)


class TestGetAllSessions(unittest.TestCase):
    """ Test that the function get_all_sessions behaves as expected.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        Doing this because one of the tests expects an empty DB.
        """
        self.db_test_priv = WFMDatabase(':memory:')
        Base.metadata.create_all(bind=self.db_test_priv.engine)

    def tearDown(self):
        self.db_test_priv.dbsession.close()

    def test_get_all_sessions_from_empty_db(self):
        """Tests that getting all sessions behaves as expected when
        the session DB is empty"""
        expected_list = []
        result = self.db_test_priv.get_all_sessions()
        self.assertListEqual(result, expected_list)

    def test_get_all_sessions_single_session(self):
        """Tests that getting all sessions behaves as expected when
        the session DB contains only one session"""
        # Add a session (DB is empty)
        session1 = Session(name='ses1', workflow_name='wkf1', user_name='user',
                           start_time=123, end_time=123, status='starting')
        self.db_test_priv.dbsession.add(session1)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.refresh(session1)
        expected_result = [{'id': session1.id, 'name': 'ses1', 'workflow_name': 'wkf1',
                            'start_time': 123, 'end_time': 123, 'status': 'starting'}]
        result = self.db_test_priv.get_all_sessions()
        self.assertListEqual(result, expected_result)

    def test_get_all_sessions_several_sessions(self):
        """Tests that getting all sessions behaves as expected when
        the session DB contains several sessions"""
        # Add 2 sessions
        session1 = Session(name='session1', workflow_name='wkf1', user_name='user',
                           start_time=123, end_time=123, status='starting')
        session2 = Session(name='session2', workflow_name='wkf1', user_name='user',
                           start_time=456, end_time=456, status='running')
        self.db_test_priv.dbsession.add(session1)
        self.db_test_priv.dbsession.add(session2)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.refresh(session1)
        self.db_test_priv.dbsession.refresh(session2)

        expected_result = [
                {'id': session1.id, 'workflow_name': 'wkf1', 'name': 'session1',
                 'start_time': 123, 'end_time': 123, 'status': 'starting'},
                {'id': session2.id, 'workflow_name': 'wkf1', 'name': 'session2',
                 'start_time': 456, 'end_time': 456, 'status': 'running'},
        ]
        result = self.db_test_priv.get_all_sessions()
        self.assertListEqual(result, expected_result)

    def test_get_all_sessions_with_user_filter(self):
        """Tests that getting all the rows from a Session table behaves as expected"""
        session1 = Session(name='session1', workflow_name='wkf1', user_name='user',
                           start_time=123, end_time=123, status='starting')
        session2 = Session(name='session1', workflow_name='wkf1', user_name='user1',
                           start_time=123, end_time=123, status='starting')
        self.db_test_priv.dbsession.add(session1)
        self.db_test_priv.dbsession.add(session2)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.refresh(session1)
        self.db_test_priv.dbsession.refresh(session2)
        expected_result = [
                {'id': session2.id, 'workflow_name': 'wkf1', 'name': 'session1',
                 'start_time': 123, 'end_time': 123, 'status': 'starting'}
        ]
        result = self.db_test_priv.get_all_sessions(uname='user1')
        self.assertListEqual(result, expected_result)
        result = self.db_test_priv.get_all_sessions(uname='unexistent_user')
        self.assertListEqual(result, [])


class TestGetSessionInfoFromName(unittest.TestCase):
    """ Test that the function get_session_info_from_name behaves as expected.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        Doing this because one of the tests adds elements to the DB.
        """
        self.db_test_priv = WFMDatabase(':memory:')
        Base.metadata.create_all(bind=self.db_test_priv.engine)

        session1 = Session(name='ses1', workflow_name='wkf1', user_name='user',
                           start_time=123, end_time=123, status='status1')
        session2 = Session(name='ses1', workflow_name='wkf1', user_name='user',
                           start_time=321, end_time=321, status='status2')
        session3 = Session(name='ses2', workflow_name='wkf1', user_name='user',
                           start_time=456, end_time=456, status='status3')
        session4 = Session(name='ses1', workflow_name='wkf4', user_name='user',
                           start_time=789, end_time=789, status='status4')
        self.db_test_priv.dbsession.add(session1)
        self.db_test_priv.dbsession.add(session2)
        self.db_test_priv.dbsession.add(session3)
        self.db_test_priv.dbsession.add(session4)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.refresh(session1)
        self.db_test_priv.dbsession.refresh(session2)
        self.db_test_priv.dbsession.refresh(session3)
        self.db_test_priv.dbsession.refresh(session4)
        self.ses_id1 = session1.id
        self.ses_id2 = session2.id
        self.ses_id3 = session3.id
        self.ses_id4 = session4.id

    def tearDown(self):
        self.db_test_priv.dbsession.close()

    def test_get_session_info_from_name_no_session0(self):
        """Tests that getting a session info by name behaves as expected when
        no session with this session name exists in the DB"""
        with self.assertRaises(UnexistingSessionNameError):
            self.db_test_priv.get_session_info_from_name('unexisting_name')

    def test_get_session_info_from_name_no_session1(self):
        """Tests that getting a session info by name behaves as expected when
        no session with these session and workflow names exists in the DB"""
        with self.assertRaises(UnexistingSessionNameError):
            self.db_test_priv.get_session_info_from_name('unexisting_sname',
                                                         'unexisting_wname')

    def test_get_session_info_from_name0(self):
        """Tests that getting a session info by name behaves as expected when
        only the session name is provided and several sessions exist
        with this name in the DB"""
        result = self.db_test_priv.get_session_info_from_name('ses1')
        expected_result = [
            {'id': self.ses_id1, 'workflow_name': 'wkf1', 'name': 'ses1',
             'start_time': 123, 'end_time': 123, 'status': 'status1'},
            {'id': self.ses_id2, 'workflow_name': 'wkf1', 'name': 'ses1',
             'start_time': 321, 'end_time': 321, 'status': 'status2'},
            {'id': self.ses_id4, 'workflow_name': 'wkf4', 'name': 'ses1',
             'start_time': 789, 'end_time': 789, 'status': 'status4'}
        ]
        self.assertListEqual(result, expected_result)

    def test_get_session_info_from_name1(self):
        """Tests that getting a session info by name behaves as expected when
        both the session and the workflow names are provided and several
        sessions with these names exist in the DB"""
        result = self.db_test_priv.get_session_info_from_name('ses1', 'wkf1')
        expected_result = [
            {'id': self.ses_id1, 'workflow_name': 'wkf1', 'name': 'ses1',
             'start_time': 123, 'end_time': 123, 'status': 'status1'},
            {'id': self.ses_id2, 'workflow_name': 'wkf1', 'name': 'ses1',
             'start_time': 321, 'end_time': 321, 'status': 'status2'}
        ]
        self.assertListEqual(result, expected_result)


class TestGetSessionInfoFromId(unittest.TestCase):
    """ Test that the function get_session_info_from_id behaves as expected.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        Doing this because one of the tests adds elements to the DB.
        """
        self.db_test_priv = WFMDatabase(':memory:')
        Base.metadata.create_all(bind=self.db_test_priv.engine)

        session1 = Session(name='ses1', workflow_name='wkf1', user_name='user',
                           start_time=123, end_time=123, status='status1')
        self.db_test_priv.dbsession.add(session1)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.refresh(session1)
        self.ses_id1 = session1.id

    def tearDown(self):
        self.db_test_priv.dbsession.close()

    def test_get_session_info_from_id_no_session(self):
        """Tests that getting a session info by id behaves as expected when
        no session with this session id exists in the DB"""
        result = self.db_test_priv.get_session_info_from_id(123456)
        expected_result = []
        self.assertListEqual(result, expected_result)

    def test_get_session_info_from_id_single_session(self):
        """Tests that getting a session info by id behaves as expected when
        a single session exists with this id in the DB"""
        result = self.db_test_priv.get_session_info_from_id(self.ses_id1)
        expected_result = [ {'id': self.ses_id1, 'workflow_name': 'wkf1', 'name': 'ses1',
                             'start_time': 123, 'end_time': 123, 'status': 'status1'}]
        self.assertListEqual(result, expected_result)


class TestDeleteSession(unittest.TestCase):
    """ Test that the function delete_session behaves as expected.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        Doing this because one of the tests adds elements to the DB
        and all of them delete tests from the DB.
        """
        self.db_test_priv = WFMDatabase(':memory:')
        Base.metadata.create_all(bind=self.db_test_priv.engine)

        session1 = Session(name='ses1', workflow_name='wkf1', user_name='user',
                           start_time=123, end_time= 123, status='status1')
        session2 = Session(name='ses2', workflow_name='wkf2', user_name='user',
                           start_time=456, end_time= 456, status='status2')
        self.db_test_priv.dbsession.add(session1)
        self.db_test_priv.dbsession.add(session2)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.refresh(session1)
        self.db_test_priv.dbsession.refresh(session2)
        self.ses_id1 = session1.id

    def tearDown(self):
        self.db_test_priv.dbsession.close()

    def test_delete_session_by_name_single_session(self):
        """Tests that deleting a session behaves as expected when
        a single session exists with this name in the DB"""
        self.db_test_priv.delete_session('ses1')
        with self.assertRaises(UnexistingSessionNameError):
            self.db_test_priv.get_session_info_from_name('ses1')
        # Check that we generated a log into the DB
        filter_txt = text("object_type == 'session'")
        result = self.db_test_priv.dbsession.query(ObjectActivityLogging).filter(filter_txt).all()
        list_dicts = [item.dict() for item in result]
        expected_result = {'id': 1, 'object_type': 'session',
                           'object_id': self.ses_id1, 'activity': 'removal'}
        # we do this instead of assertListEqual because we do not control the time
        # the activity occurred at
        self.assertEqual(list_dicts[0]['object_type'], expected_result['object_type'])
        self.assertEqual(list_dicts[0]['object_id'], expected_result['object_id'])
        self.assertEqual(list_dicts[0]['activity'], expected_result['activity'])

    def test_delete_session_by_name_several_sessions(self):
        """Tests that deleting a session behaves as expected when
        several sessions with the same name exist in the DB"""
        # Add another session with the same name
        session3 = Session(name='ses1', workflow_name='wkf3', user_name='user',
                           start_time=789, end_time=789, status='status3')
        self.db_test_priv.dbsession.add(session3)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.refresh(session3)
        # Check we have both sessions for the same name
        result = self.db_test_priv.get_session_info_from_name('ses1')
        expected_result = [{'id': self.ses_id1, 'workflow_name': 'wkf1', 'name': 'ses1',
                            'start_time': 123, 'end_time': 123, 'status': 'status1'},
                           {'id': session3.id, 'workflow_name': 'wkf3', 'name': 'ses1',
                            'start_time': 789, 'end_time': 789, 'status': 'status3'}]
        self.assertListEqual(result, expected_result)
        self.db_test_priv.delete_session('ses1')
        with self.assertRaises(UnexistingSessionNameError):
            self.db_test_priv.get_session_info_from_name('ses1')
        # Check that we generated 2 logs into the DB
        filter_txt = text("object_type == 'session'")
        result = self.db_test_priv.dbsession.query(ObjectActivityLogging).filter(filter_txt).all()
        list_dicts = [item.dict() for item in result]
        expected_result = [{'id': 1, 'object_type': 'session',
                            'object_id': self.ses_id1, 'activity': 'removal'},
                           {'id': 2, 'object_type': 'session',
                            'object_id': session3.id, 'activity': 'removal'}]
        # we do this instead of assertListEqual because we do not control the time
        # the activity occured at
        for idx in (0, 1):
            self.assertEqual(list_dicts[idx]['object_type'], expected_result[idx]['object_type'])
            self.assertEqual(list_dicts[idx]['object_id'], expected_result[idx]['object_id'])
            self.assertEqual(list_dicts[idx]['activity'], expected_result[idx]['activity'])

    def test_delete_session_by_id_no_session(self):
        """Tests that deleting a session by id behaves as expected when
        no session exists with this id in the DB"""
        self.db_test_priv.delete_session(session_id=self.ses_id1)
        with self.assertRaises(UnexistingSessionNameError):
            self.db_test_priv.get_session_info_from_name('ses1')
        # Check that we generated a log into the DB
        filter_txt = text("object_type == 'session'")
        result = self.db_test_priv.dbsession.query(ObjectActivityLogging).filter(filter_txt).all()
        list_dicts = [item.dict() for item in result]
        expected_result = {'id': 1, 'object_type': 'session',
                           'object_id': self.ses_id1, 'activity': 'removal'}
        # we do this instead of assertListEqual because we do not control the time
        # the activity occured at
        self.assertEqual(list_dicts[0]['object_type'], expected_result['object_type'])
        self.assertEqual(list_dicts[0]['object_id'], expected_result['object_id'])
        self.assertEqual(list_dicts[0]['activity'], expected_result['activity'])

    def test_delete_session_by_id_single_session(self):
        """Tests that deleting a session by id behaves as expected when
        a session exists with this id in the DB"""
        self.db_test_priv.delete_session(session_id=self.ses_id1)
        with self.assertRaises(UnexistingSessionNameError):
            self.db_test_priv.get_session_info_from_name('ses1')
        # Check that we generated a log into the DB
        filter_txt = text("object_type == 'session'")
        result = self.db_test_priv.dbsession.query(ObjectActivityLogging).filter(filter_txt).all()
        list_dicts = [item.dict() for item in result]
        expected_result = {'id': 1, 'object_type': 'session',
                           'object_id': self.ses_id1, 'activity': 'removal'}
        # we do this instead of assertListEqual because we do not control the time
        # the activity occured at
        self.assertEqual(list_dicts[0]['object_type'], expected_result['object_type'])
        self.assertEqual(list_dicts[0]['object_id'], expected_result['object_id'])
        self.assertEqual(list_dicts[0]['activity'], expected_result['activity'])

    def test_delete_session_by_name_and_id(self):
        """Tests that deleting a session behaves as expected when
        a single session exists with this name in the DB and both the name and an id are provided"""
        self.db_test_priv.delete_session('ses1', 123)
        with self.assertRaises(UnexistingSessionNameError):
            self.db_test_priv.get_session_info_from_name('ses1')
        # Check that we generated a log into the DB
        filter_txt = text("object_type == 'session'")
        result = self.db_test_priv.dbsession.query(ObjectActivityLogging).filter(filter_txt).all()
        list_dicts = [item.dict() for item in result]
        expected_result = {'id': 1, 'object_type': 'session',
                           'object_id': self.ses_id1, 'activity': 'removal'}
        # we do this instead of assertListEqual because we do not control the time
        # the activity occurred at
        self.assertEqual(list_dicts[0]['object_type'], expected_result['object_type'])
        self.assertEqual(list_dicts[0]['object_id'], expected_result['object_id'])
        self.assertEqual(list_dicts[0]['activity'], expected_result['activity'])


class TestUpdateSessionStatus(unittest.TestCase):
    """ Test that the function update_session_status behaves as expected.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        Doing this because one of the tests adds elements to the DB.
        """
        self.db_test_priv = WFMDatabase(':memory:')
        Base.metadata.create_all(bind=self.db_test_priv.engine)

        session1 = Session(name='ses1', workflow_name='wkf1', user_name='user',
                           start_time=123, end_time=123, status='status1')
        self.db_test_priv.dbsession.add(session1)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.refresh(session1)
        self.ses_id1 = session1.id

    def tearDown(self):
        self.db_test_priv.dbsession.close()

    def test_update_session_status_single_session(self):
        """Tests that updating a session status behaves as expected when
        a single session exists with this name in the DB"""
        self.db_test_priv.update_session_status('ses1', 'stopped')
        result = self.db_test_priv.get_session_info_from_name('ses1')
        expected_result = [{'id': self.ses_id1, 'workflow_name': 'wkf1', 'name': 'ses1',
                            'start_time': 123, 'end_time': 123, 'status': 'stopped'}]
        self.assertListEqual(result, expected_result)

    def test_update_session_status_several_sessions(self):
        """Tests that updating a session status behaves as expected when
        several sessions with the same name exist in the DB"""
        # Add another session with the same name
        session2 = Session(name='ses1', workflow_name='wkf2', user_name='user',
                           start_time=456, end_time=456, status='status2')
        self.db_test_priv.dbsession.add(session2)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.refresh(session2)
        self.db_test_priv.update_session_status('ses1', 'stopped')
        result = self.db_test_priv.get_session_info_from_name('ses1')
        expected_result = [{'id': self.ses_id1, 'workflow_name': 'wkf1', 'name': 'ses1',
                            'start_time': 123, 'end_time': 123, 'status': 'stopped'},
                           {'id': session2.id, 'workflow_name': 'wkf2', 'name': 'ses1',
                            'start_time': 456, 'end_time': 456, 'status': 'stopped'}]
        self.assertListEqual(result, expected_result)


class TestAddNsLock(unittest.TestCase):
    """ Test that the function add_nslock behaves as expected.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        """
        self.db_test_priv = WFMDatabase(':memory:')
        Base.metadata.create_all(bind=self.db_test_priv.engine)

        ns1 = NamespaceLock(ns_name="ns1", service_name="srv1")
        ns2 = NamespaceLock(ns_name="ns2", service_name="srv2")

        self.db_test_priv.dbsession.add(ns1)
        self.db_test_priv.dbsession.add(ns2)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.refresh(ns1)
        self.db_test_priv.dbsession.refresh(ns2)

        # ids of the already stored namespaces
        self.ids = [ ns1.id, ns2.id ]

    def tearDown(self):
        self.db_test_priv.dbsession.close()

    def test_add_nslock_unexisting_nslock(self):
        """Tests that adding a not yet existing namespace lock behaves as expected"""
        result = self.db_test_priv.add_nslock('ns3', 'srv3')
        self.assertNotIn(result, self.ids)

    def test_add_nslock_existing_nslock(self):
        """Tests that adding an already existing namespace lock behaves as expected"""
        result1 = self.db_test_priv.add_nslock('ns4', 'srv4')
        self.assertNotIn(result1, self.ids)
        # Add it a 2nd time, it should be added with a different id
        result2 = self.db_test_priv.add_nslock('ns4', 'srv4')
        self.assertNotEqual(result2, result1)


class TestDeleteNsLock(unittest.TestCase):
    """ Test that the function delete_nslock behaves as expected.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        """
        self.db_test_priv = WFMDatabase(':memory:')
        Base.metadata.create_all(bind=self.db_test_priv.engine)

        self.ns1 = NamespaceLock(ns_name="ns1", service_name="srv1")
        self.ns2 = NamespaceLock(ns_name="ns2", service_name="srv2")

        self.db_test_priv.dbsession.add(self.ns1)
        self.db_test_priv.dbsession.add(self.ns2)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.refresh(self.ns1)
        self.db_test_priv.dbsession.refresh(self.ns2)
        self.ns_id1 = self.ns1.id

    def tearDown(self):
        self.db_test_priv.dbsession.close()

    def test_delete_nslock_no_nslock(self):
        """Tests that deleting a namespace lock behaves as expected when
        no namespace exists with this name in the DB"""
        with self.assertRaises(NoDocumentError):
            self.db_test_priv.delete_nslock('UNKNOWN')

    def test_delete_nslock_single_nslock(self):
        """Tests that deleting a namespace lock behaves as expected when
        a single namespace exists with this name in the DB"""
        self.db_test_priv.delete_nslock(self.ns1.ns_name)
        result = self.db_test_priv.get_ns_info_from_name(self.ns1.ns_name)
        expected_result = []
        self.assertListEqual(result, expected_result)

    def test_delete_nslock_several_nslocks(self):
        """Tests that deleting a namespace lock behaves as expected when
        several namespaces with the same name exist in the DB"""
        # Add another namespace with the same name
        ns3 = NamespaceLock(ns_name=self.ns2.ns_name, service_name="srv3")
        self.db_test_priv.dbsession.add(ns3)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.refresh(ns3)

        # Check we have both services for the same name
        expected_result = [{'id': self.ns2.id,
                            'ns_name': self.ns2.ns_name,
                            'service_name': self.ns2.service_name},
                           {'id': ns3.id, 'ns_name': ns3.ns_name,
                            'service_name': ns3.service_name}]
        result = self.db_test_priv.get_ns_info_from_name(self.ns2.ns_name)
        self.assertListEqual(result, expected_result)

        with self.assertRaises(NoUniqueDocumentError):
            self.db_test_priv.delete_nslock(self.ns2.ns_name)


class TestGetNsInfoFromName(unittest.TestCase):
    """ Test that the function get_ns_info_from_name behaves as expected.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        """
        self.db_test_priv = WFMDatabase(':memory:')
        Base.metadata.create_all(bind=self.db_test_priv.engine)

        self.ns1 = NamespaceLock(ns_name="ns1", service_name="srv1")
        self.ns2 = NamespaceLock(ns_name="ns2", service_name="srv2")

        self.db_test_priv.dbsession.add(self.ns1)
        self.db_test_priv.dbsession.add(self.ns2)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.refresh(self.ns1)
        self.db_test_priv.dbsession.refresh(self.ns2)
        self.ns_id1 = self.ns1.id

    def tearDown(self):
        self.db_test_priv.dbsession.close()

    def test_get_ns_no_ns(self):
        """Tests that getting a namespace behaves as expected when
        no namespace exists with this name in the DB"""
        result = self.db_test_priv.get_ns_info_from_name('UNKNOWN')
        expected_result = []
        self.assertListEqual(result, expected_result)

    def test_get_ns_single_ns(self):
        """Tests that getting a namespace behaves as expected when
        a single namespace exists with this name in the DB"""
        result = self.db_test_priv.get_ns_info_from_name(self.ns1.ns_name)
        expected_result = [{'id': self.ns1.id,
                            'ns_name': self.ns1.ns_name,
                            'service_name': self.ns1.service_name}]
        self.assertListEqual(result, expected_result)

    def test_get_ns_several_namespaces(self):
        """Tests that getting a namespace behaves as expected when
        several namespaces with the same name exist in the DB"""
        # Add another namespace with the same name
        ns3 = NamespaceLock(ns_name=self.ns2.ns_name, service_name="srv3")
        self.db_test_priv.dbsession.add(ns3)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.refresh(ns3)

        # Check we have both services for the same name
        expected_result = [{'id': self.ns2.id,
                            'ns_name': self.ns2.ns_name,
                            'service_name': self.ns2.service_name},
                           {'id': ns3.id, 'ns_name': ns3.ns_name,
                            'service_name': ns3.service_name}]
        result = self.db_test_priv.get_ns_info_from_name(self.ns2.ns_name)
        self.assertListEqual(result, expected_result)


class TestGetServicesFromNs(unittest.TestCase):
    """ Test that the function get_services_from_ns behaves as expected.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        """
        self.db_test_priv = WFMDatabase(':memory:')
        Base.metadata.create_all(bind=self.db_test_priv.engine)

        self.ns1 = NamespaceLock(ns_name="ns1", service_name="srv1")
        self.ns2 = NamespaceLock(ns_name="ns2", service_name="srv2")

        self.db_test_priv.dbsession.add(self.ns1)
        self.db_test_priv.dbsession.add(self.ns2)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.refresh(self.ns1)
        self.db_test_priv.dbsession.refresh(self.ns2)
        self.ns_id1 = self.ns1.id

    def tearDown(self):
        self.db_test_priv.dbsession.close()

    def test_get_services_ns_no_ns(self):
        """Tests that getting services from a namespace behaves as expected when
        no namespace exists with this name in the DB"""
        result = self.db_test_priv.get_services_from_ns('UNKNOWN')
        expected_result = []
        self.assertListEqual(result, expected_result)

    def test_get_services_ns_single_ns(self):
        """Tests that getting services from a namespace behaves as expected when
        a single namespace exists with this name in the DB"""
        result = self.db_test_priv.get_services_from_ns(self.ns1.ns_name)
        expected_result = [ self.ns1.service_name ]
        self.assertListEqual(result, expected_result)

    def test_get_services_ns_several_namespaces(self):
        """Tests that getting services from a namespace behaves as expected when
        several namespaces with the same name exist in the DB"""
        # Add another namespace with the same name
        ns3 = NamespaceLock(ns_name=self.ns2.ns_name, service_name="srv3")
        self.db_test_priv.dbsession.add(ns3)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.refresh(ns3)

        # Check we have both services for the same name
        expected_result = [ self.ns2.service_name, ns3.service_name ]
        result = self.db_test_priv.get_services_from_ns(self.ns2.ns_name)
        self.assertListEqual(result, expected_result)


class TestAddUniqueService(unittest.TestCase):
    """ Test that the function add_unique_service behaves as expected.
    """
    def setUp(self):
        """Connect to database for tests.
        """
        self.db_test = WFMDatabase(name=TEST_DATABASE)

        # ids of the already stored services (see __init__.py)
        self.ids = [ 1, 2 ]

    def tearDown(self):
        self.db_test.dbsession.close()

    def test_add_unique_service_unexisting_service(self):
        """Tests that adding a not yet existing service behaves as expected"""
        unexisting_srv_dict = {
            'name': 'srv2',
            'session_id': 1,
            'type': 'SBB',
            'attributes': {
                'targets': '/target2',
                'flavor': 'small',
                'datanodes': 4
            }
        }
        result = self.db_test.add_unique_service(unexisting_srv_dict, 456, 'allocated', 111)
        self.assertNotIn(result, self.ids)
        # Check that we generated a log into the DB
        query = f"object_type == 'service' AND object_id == {result}"
        result_query = self.db_test.dbsession.query(ObjectActivityLogging).filter(text(query)).all()
        list_dicts = [item.dict() for item in result_query]
        self.assertEqual(len(list_dicts), 1)
        expected_result = {'id': 1, 'object_type': 'service',
                            'object_id': result, 'activity': 'creation'}
        # we do this instead of assertListEqual because the id might not be '1' since we already
        # added some logs in the init
        self.assertEqual(list_dicts[0]['object_type'], expected_result['object_type'])
        self.assertEqual(list_dicts[0]['object_id'], expected_result['object_id'])
        self.assertEqual(list_dicts[0]['activity'], expected_result['activity'])

    def test_add_unique_service_existing_service(self):
        """Tests that adding an already existing service behaves as expected"""
        existing_srv_dict = {
            'session_id': 1,
            'name': 'e1',
            'type': 'SBB',
            'attributes': {
                'targets': '/target1',
                'flavor': 'flavor1',
                'datanodes': 1 },
            'status': 'status1'
        }

        result = self.db_test.add_unique_service(existing_srv_dict, 0, 'status1', 1)
        self.assertEqual(result, 1)


class TestAddService(unittest.TestCase):
    """ Test that the function add_service behaves as expected.
    """
    def setUp(self):
        """Connect to database for tests.
        """
        self.db_test = WFMDatabase(name=TEST_DATABASE)

        # ids of the already stored services (see __init__.py)
        self.ids = [ 1, 2 ]

    def tearDown(self):
        self.db_test.dbsession.close()

    def test_add_service_unexisting_service(self):
        """Tests that adding a not yet existing service behaves as expected"""
        unexisting_srv_dict = {
            'name': 'srv2',
            'session_id': 1,
            'type': 'SBB',
            'attributes': {
                'targets': '/target2',
                'flavor': 'small',
                'datanodes': 4
            }
        }
        result = self.db_test.add_service(unexisting_srv_dict, 456, 'allocated', 111)
        self.assertNotIn(result, self.ids)
        # Check that we generated a log into the DB
        query = f"object_type == 'service' AND object_id == {result}"
        result_query = self.db_test.dbsession.query(ObjectActivityLogging).filter(text(query)).all()
        list_dicts = [item.dict() for item in result_query]
        self.assertEqual(len(list_dicts), 1)
        expected_result = {'id': 1, 'object_type': 'service',
                           'object_id': result, 'activity': 'creation'}
        # we do this instead of assertListEqual because the id might not be '1' since we already
        # added some logs in the init
        self.assertEqual(list_dicts[0]['object_type'], expected_result['object_type'])
        self.assertEqual(list_dicts[0]['object_id'], expected_result['object_id'])
        self.assertEqual(list_dicts[0]['activity'], expected_result['activity'])

    def test_add_service_existing_service(self):
        """Tests that adding an already existing service behaves as expected"""
        unexisting_srv_dict = { 'name': 'unexisting_srv', 'session_id': 1, 'type': 'SBB',
                                'attributes': {
                                    'targets': '/target2',
                                    'flavor': 'small',
                                    'datanodes': 4 } }
        result1 = self.db_test.add_service(unexisting_srv_dict, 456, 'allocated', 111)
        self.assertNotIn(result1, self.ids)
        # Add it a 2nd time, it should be added with a different id
        result2 = self.db_test.add_service(unexisting_srv_dict, 456, 'allocated', 111)
        self.assertNotEqual(result2, result1)


class TestGetAllServices(unittest.TestCase):
    """ Test that the function get_allservices behaves as expected.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        Doing this because one of the tests expects an empty DB.
        """
        self.db_test_priv = WFMDatabase(':memory:')
        Base.metadata.create_all(bind=self.db_test_priv.engine)

    def tearDown(self):
        self.db_test_priv.dbsession.close()

    def test_get_all_services_from_empty_db(self):
        """Tests that getting all services behaves as expected when
        the service DB is empty"""
        expected_list = []
        result = self.db_test_priv.get_all_services()
        self.assertListEqual(result, expected_list)

    def test_get_all_services_single_service(self):
        """Tests that getting all services behaves as expected when
        the service DB is not empty"""
        # Add a service (DB is empty)
        service1 = Service(session_id=1, name='srv1', service_type='SBB', location='location1',
                           targets='/tmp', flavor='small', datanodes=4,
                           start_time=123, end_time=123, status='allocated', jobid=1)
        self.db_test_priv.dbsession.add(service1)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.refresh(service1)
        result = self.db_test_priv.get_all_services()
        expected_result = [{'id': service1.id, 'session_id': 1, 'name': 'srv1', 'type': 'SBB',
                            'location': 'location1', 'targets': '/tmp', 'status': 'allocated',
                            'jobid': 1}]
        self.assertListEqual(result, expected_result)

    def test_get_all_services_several_service(self):
        """Tests that getting all services behaves as expected when
        the service DB is not empty"""
        # Add 2 services (DB is empty)
        service1 = Service(session_id=1, name='srv1', service_type='SBB', location='location1',
                           targets='/tmp', flavor='small', datanodes=4,
                           start_time=123, end_time=123, status='allocated', jobid=1)
        service2 = Service(session_id=2, name='srv2', service_type='SBB', location='location2',
                           targets='/tmp', flavor='small', datanodes=4,
                           start_time=123, end_time=123, status='allocated', jobid=2)
        self.db_test_priv.dbsession.add(service1)
        self.db_test_priv.dbsession.add(service2)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.refresh(service1)
        self.db_test_priv.dbsession.refresh(service2)
        result = self.db_test_priv.get_all_services()
        expected_result = []
        expected_result = [{'id': service1.id, 'session_id': 1, 'name': 'srv1', 'type': 'SBB',
                            'location': 'location1', 'targets': '/tmp', 'status': 'allocated',
                            'jobid': 1},
                           {'id': service2.id, 'session_id': 2, 'name': 'srv2', 'type': 'SBB',
                            'location': 'location2', 'targets': '/tmp', 'status': 'allocated',
                            'jobid': 2}]
        self.assertListEqual(result, expected_result)


class TestGetServiceInfoFromName(unittest.TestCase):
    """ Test that the function get_service_info_from_name behaves as expected.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        Doing this because one of the tests adds elements to the DB.
        """
        self.db_test_priv = WFMDatabase(':memory:')
        Base.metadata.create_all(bind=self.db_test_priv.engine)

        service1 = Service(session_id=1, name='srv1', service_type='SBB',
                           location='location1',
                           targets='/tmp', flavor='small', datanodes=4,
                           start_time=123, end_time=123, status='allocated', jobid=1)
        self.db_test_priv.dbsession.add(service1)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.refresh(service1)
        self.srv_id1 = service1.id

    def tearDown(self):
        self.db_test_priv.dbsession.close()

    def test_get_service_info_from_name_no_service(self):
        """Tests that getting a service info by name behaves as expected when
        no service with this name exists in the DB"""
        with self.assertRaises(UnexistingServiceNameError):
            self.db_test_priv.get_service_info_from_name('unexisting_name')

    def test_get_service_info_from_name_single_service(self):
        """Tests that getting a service info by name behaves as expected when
        a single service exists with this name in the DB"""
        result = self.db_test_priv.get_service_info_from_name('srv1')
        expected_result = [{'id': self.srv_id1, 'session_id': 1, 'name': 'srv1',
                            'type': 'SBB', 'location': 'location1', 'targets': '/tmp',
                            'status': 'allocated', 'jobid': 1}]
        self.assertListEqual(result, expected_result)

    def test_get_service_info_from_name_several_services(self):
        """Tests that getting a service info by name behaves as expected when
        several services with the same name exist in the DB"""
        # Add another service with the same name to the DB
        service2 = Service(session_id=2, name='srv1', service_type='SBB',
                           location='location2',
                           targets='/tmp2', flavor='medium', datanodes=4,
                           start_time=456, end_time=456, status='allocated', jobid=2)
        self.db_test_priv.dbsession.add(service2)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.refresh(service2)
        result = self.db_test_priv.get_service_info_from_name('srv1')
        expected_result = [{'id': self.srv_id1, 'session_id': 1, 'name': 'srv1',
                            'type': 'SBB', 'location': 'location1', 'targets': '/tmp',
                            'status': 'allocated', 'jobid': 1},
                           {'id': service2.id, 'session_id': 2, 'name': 'srv1',
                            'type': 'SBB', 'location': 'location2', 'targets': '/tmp2',
                            'status': 'allocated', 'jobid': 2}]
        self.assertListEqual(result, expected_result)


class TestGetServicesInfoFromSessionId(unittest.TestCase):
    """ Test that the function get_services_info_from_session_id behaves as expected.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        Doing this because one of the tests adds elements to the DB.
        """
        self.db_test_priv = WFMDatabase(':memory:')
        Base.metadata.create_all(bind=self.db_test_priv.engine)

        self.service1 = Service(session_id=1, name='srv1', service_type='SBB',
                                location='location1',
                                targets='target1', flavor='flavor1', datanodes=4,
                                start_time=123, end_time=123, status='status1', jobid=1)
        self.db_test_priv.dbsession.add(self.service1)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.refresh(self.service1)
        self.srv_id1 = self.service1.id
        self.ses_id1 = self.service1.session_id

    def tearDown(self):
        self.db_test_priv.dbsession.delete(self.service1)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.close()

    def test_get_services_info_from_session_id_no_service(self):
        """Tests that getting services info by session id behaves as expected when
        no service with this session id exists in the DB"""
        result = self.db_test_priv.get_services_info_from_session_id(123)
        self.assertListEqual(result, [])

    def test_get_services_info_from_session_id_single_service(self):
        """Tests that getting services info by session id behaves as expected when
        a single service exists with this session id in the DB"""
        result = self.db_test_priv.get_services_info_from_session_id(self.ses_id1)
        expected_result = [{'id': self.srv_id1, 'session_id': self.ses_id1, 'name': 'srv1',
                            'type': 'SBB', 'location': 'location1', 'targets': 'target1',
                            'status': 'status1', 'jobid': 1}]
        self.assertListEqual(result, expected_result)

    def test_get_service_info_from_session_id_several_services(self):
        """Tests that getting services info by session id behaves as expected when
        several services with the same session id exist in the DB"""
        # Add another service with the same session id to the DB
        service2 = Service(session_id=self.ses_id1, name='srv2', service_type='SBB',
                           location='location2',
                           targets='target2', flavor='flavor2', datanodes=4,
                           start_time=456, end_time=456, status='status2', jobid=2)
        self.db_test_priv.dbsession.add(service2)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.refresh(service2)
        result = self.db_test_priv.get_services_info_from_session_id(self.ses_id1)
        expected_result = [{'id': self.srv_id1, 'session_id': self.ses_id1, 'name': 'srv1',
                            'type': 'SBB', 'location': 'location1', 'targets': 'target1',
                            'status': 'status1', 'jobid': 1},
                           {'id': service2.id, 'session_id': self.ses_id1, 'name': 'srv2',
                            'type': 'SBB', 'location': 'location2', 'targets': 'target2',
                            'status': 'status2', 'jobid': 2}]
        self.assertListEqual(result, expected_result)
        self.db_test_priv.dbsession.delete(service2)
        self.db_test_priv.dbsession.commit()


class TestGetServiceInfoFromId(unittest.TestCase):
    """ Test that the function get_service_info_from_id behaves as expected.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        Doing this because one of the tests adds elements to the DB.
        """
        self.db_test_priv = WFMDatabase(':memory:')
        Base.metadata.create_all(bind=self.db_test_priv.engine)

        self.service1 = Service(session_id=1, name='srv1', service_type='SBB',
                           location='location1',
                           targets='target1', flavor='flavor1', datanodes=4,
                           start_time=123, end_time=123, status='status1', jobid=1)
        self.db_test_priv.dbsession.add(self.service1)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.refresh(self.service1)
        self.srv_id1 = self.service1.id
        self.ses_id1 = self.service1.session_id

    def tearDown(self):
        self.db_test_priv.dbsession.delete(self.service1)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.close()

    def test_get_service_info_from_id_no_service(self):
        """Tests that getting services info by session id behaves as expected when
        no service with this session id exists in the DB"""
        result = self.db_test_priv.get_service_info_from_id(123)
        self.assertListEqual(result, [])

    def test_get_service_info_from_id_single_service(self):
        """Tests that getting services info by session id behaves as expected when
        a single service exists with this session id in the DB"""
        result = self.db_test_priv.get_service_info_from_id(self.srv_id1)
        expected_result = [{'id': self.srv_id1, 'session_id': self.ses_id1, 'name': 'srv1',
                            'type': 'SBB', 'location': 'location1', 'targets': 'target1',
                            'status': 'status1', 'jobid': 1}]
        self.assertListEqual(result, expected_result)


class TestDeleteService(unittest.TestCase):
    """ Test that the function delete_service behaves as expected.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        Doing this because one of the tests adds elements to the DB
        and all of them delete tests from the DB.
        """
        self.db_test_priv = WFMDatabase(':memory:')
        Base.metadata.create_all(bind=self.db_test_priv.engine)

        service1 = Service(session_id=1, name='srv1', service_type='SBB',
                           location='location1',
                           targets='/tmp', flavor='small', datanodes=4,
                           start_time=123, end_time=123, status='allocated', jobid=1)
        service2 = Service(session_id=2, name='srv2', service_type='SBB',
                           location='location2',
                           targets='/tmp2', flavor='small', datanodes=4,
                           start_time=456, end_time=456, status='allocated', jobid=2)
        self.db_test_priv.dbsession.add(service1)
        self.db_test_priv.dbsession.add(service2)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.refresh(service1)
        self.db_test_priv.dbsession.refresh(service2)
        self.srv_id1 = service1.id

    def tearDown(self):
        self.db_test_priv.dbsession.close()

    def test_delete_service_single_service(self):
        """Tests that deleting a service behaves as expected when
        a single service exists with this name in the DB"""
        self.db_test_priv.delete_service('srv1')
        with self.assertRaises(UnexistingServiceNameError):
            self.db_test_priv.get_service_info_from_name('srv1')
        # Check that we generated a log into the DB
        filter_txt = text("object_type == 'service'")
        result = self.db_test_priv.dbsession.query(ObjectActivityLogging).filter(filter_txt).all()
        list_dicts = [item.dict() for item in result]
        expected_result = {'id': 1, 'object_type': 'service',
                           'object_id': self.srv_id1, 'activity': 'removal'}
        # we do this instead of assertListEqual because the id might not be '1' since we already
        # added some logs in the init
        self.assertEqual(list_dicts[0]['object_type'], expected_result['object_type'])
        self.assertEqual(list_dicts[0]['object_id'], expected_result['object_id'])
        self.assertEqual(list_dicts[0]['activity'], expected_result['activity'])

    def test_delete_service_several_services(self):
        """Tests that deleting a service behaves as expected when
        several services with the same name exist in the DB"""
        # Add another service with the same name
        service3 = Service(session_id=3, name='srv1', service_type='SBB', location='location3',
                           targets='/tmp3', flavor='small', datanodes=4,
                           start_time=789, end_time=789, status='allocated', jobid=3)
        self.db_test_priv.dbsession.add(service3)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.refresh(service3)
        # Check we have both services for the same name
        result = self.db_test_priv.get_service_info_from_name('srv1')
        expected_result = [{'id': self.srv_id1, 'session_id': 1, 'name': 'srv1',
                            'type': 'SBB', 'location': 'location1', 'targets': '/tmp',
                            'status': 'allocated', 'jobid': 1},
                           {'id': service3.id, 'session_id': 3, 'name': 'srv1',
                            'type': 'SBB', 'location': 'location3', 'targets': '/tmp3',
                            'status': 'allocated', 'jobid': 3}]
        self.assertListEqual(result, expected_result)
        self.db_test_priv.delete_service('srv1')
        with self.assertRaises(UnexistingServiceNameError):
            self.db_test_priv.get_service_info_from_name('srv1')
        # Check that we generated 2 logs into the DB
        filter_txt = text("object_type == 'service'")
        result = self.db_test_priv.dbsession.query(ObjectActivityLogging).filter(filter_txt).all()
        list_dicts = [item.dict() for item in result]
        expected_result = [{'id': 1, 'object_type': 'service',
                            'object_id': self.srv_id1, 'activity': 'removal'},
                           {'id': 2, 'object_type': 'service',
                            'object_id': service3.id, 'activity': 'removal'}]
        # we do this instead of assertListEqual because the id might not be '1' since we already
        # added some logs in the init
        for idx in (0, 1):
            self.assertEqual(list_dicts[idx]['object_type'], expected_result[idx]['object_type'])
            self.assertEqual(list_dicts[idx]['object_id'], expected_result[idx]['object_id'])
            self.assertEqual(list_dicts[idx]['activity'], expected_result[idx]['activity'])


class TestUpdateServiceSessionID(unittest.TestCase):
    """ Test that the function update_service_sessionid behaves as expected.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        Doing this because one of the tests adds elements to the DB.
        """
        self.db_test_priv = WFMDatabase(':memory:')
        Base.metadata.create_all(bind=self.db_test_priv.engine)

        service1 = Service(session_id=1, name='srv1', service_type='SBB',
                           location='location1',
                           targets='/tmp', flavor='small', datanodes=4,
                           start_time=123, end_time=123, status='allocated', jobid=1)
        self.db_test_priv.dbsession.add(service1)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.refresh(service1)
        self.srv_id1 = service1.id

    def tearDown(self):
        self.db_test_priv.dbsession.close()

    def test_update_service_sessionid_single_service(self):
        """Tests that updating a service session id behaves as expected when
        a single service exists with this name in the DB"""
        self.db_test_priv.update_service_sessionid('srv1', 2)
        result = self.db_test_priv.get_service_info_from_name('srv1')
        expected_result = [{'id': self.srv_id1, 'session_id': 2, 'name': 'srv1',
                            'type': 'SBB', 'location': 'location1', 'targets': '/tmp',
                            'status': 'allocated', 'jobid': 1}]
        self.assertListEqual(result, expected_result)

    def test_update_service_sessionid_several_services(self):
        """Tests that updating a service session id behaves as expected when
        several services with the same name exist in the DB"""
        # Add another service with the same name
        service2 = Service(session_id=2, name='srv1', service_type='SBB',
                           location='location2',
                           targets='/tmp2', flavor='small', datanodes=4,
                           start_time=456, end_time=456, status='allocated', jobid=2)
        self.db_test_priv.dbsession.add(service2)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.refresh(service2)
        self.db_test_priv.update_service_sessionid('srv1', 3)
        result = self.db_test_priv.get_service_info_from_name('srv1')
        expected_result = [{'id': self.srv_id1, 'session_id': 3, 'name': 'srv1',
                            'type': 'SBB', 'location': 'location1', 'targets': '/tmp',
                            'status': 'allocated', 'jobid': 1},
                           {'id': service2.id, 'session_id': 3, 'name': 'srv1',
                            'type': 'SBB', 'location': 'location2', 'targets': '/tmp2',
                            'status': 'allocated', 'jobid': 2}]
        self.assertListEqual(result, expected_result)


class TestUpdateServiceStatus(unittest.TestCase):
    """ Test that the function update_service_status behaves as expected.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        Doing this because one of the tests adds elements to the DB.
        """
        self.db_test_priv = WFMDatabase(':memory:')
        Base.metadata.create_all(bind=self.db_test_priv.engine)

        service1 = Service(session_id=1, name='srv1', service_type='SBB',
                           location='',
                           targets='/tmp', flavor='small', datanodes=4,
                           start_time=123, end_time=123, status='allocated', jobid=1)
        self.db_test_priv.dbsession.add(service1)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.refresh(service1)
        self.srv_id1 = service1.id

    def tearDown(self):
        self.db_test_priv.dbsession.close()

    def test_update_service_status_single_service(self):
        """Tests that updating a service status behaves as expected when
        a single service exists with this name in the DB"""
        self.db_test_priv.update_service_status('srv1', 'stopped')
        result = self.db_test_priv.get_service_info_from_name('srv1')
        expected_result = [{'id': self.srv_id1, 'session_id': 1, 'name': 'srv1',
                            'type': 'SBB', 'location': '', 'targets': '/tmp', 'status': 'stopped',
                            'jobid': 1}]
        self.assertListEqual(result, expected_result)

    def test_update_service_status_several_services(self):
        """Tests that updating a service status behaves as expected when
        several services with the same name exist in the DB"""
        # Add another service with the same name
        service2 = Service(session_id=2, name='srv1', service_type='SBB', location='',
                           targets='/tmp2', flavor='small', datanodes=4,
                           start_time=456, end_time=456, status='allocating', jobid=2)
        self.db_test_priv.dbsession.add(service2)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.refresh(service2)
        self.db_test_priv.update_service_status('srv1', 'stopped')
        result = self.db_test_priv.get_service_info_from_name('srv1')
        expected_result = [{'id': self.srv_id1, 'session_id': 1, 'name': 'srv1',
                            'type': 'SBB', 'location': '', 'targets': '/tmp', 'status': 'stopped',
                            'jobid': 1},
                           {'id': service2.id, 'session_id': 2, 'name': 'srv1',
                            'type': 'SBB', 'location': '', 'targets': '/tmp2', 'status': 'stopped',
                            'jobid': 2}]
        self.assertListEqual(result, expected_result)


class TestAddUniqueStepDescription(unittest.TestCase):
    """ Test that the function add_unique_step_description behaves as expected.
    """
    def setUp(self):
        """Connect to database for tests.
        """
        self.db_test = WFMDatabase(name=TEST_DATABASE)

        # ids of the already stored step descriptions (see __init__.py)
        self.ids = [ 1, 2 ]

    def tearDown(self):
        self.db_test.dbsession.close()

    def test_add_unique_step_description_unexisting_step(self):
        """Tests that adding a not yet existing step description behaves as expected"""
        result = self.db_test.add_unique_step_description(2, 'unexisting_name',
                                                          'fake_command', 'e2')
        self.assertNotIn(result, self.ids)
        # Check that we generated a log into the DB
        query = f"object_type == 'step_description' AND object_id == {result}"
        result_query = self.db_test.dbsession.query(ObjectActivityLogging).filter(text(query)).all()
        list_dicts = [item.dict() for item in result_query]
        self.assertEqual(len(list_dicts), 1)
        expected_result = {'id': 1, 'object_type': 'step_description',
                           'object_id': result, 'activity': 'creation'}
        # we do this instead of assertListEqual because the id might not be '1' since we already
        # added some logs in the init
        self.assertEqual(list_dicts[0]['object_type'], expected_result['object_type'])
        self.assertEqual(list_dicts[0]['object_id'], expected_result['object_id'])
        self.assertEqual(list_dicts[0]['activity'], expected_result['activity'])

    def test_add_unique_step_description_existing_step1(self):
        """Tests that adding an already existing step description (name and session id identical)
        behaves as expected"""
        result = self.db_test.add_unique_step_description(1, 'a', 'fake_command', 'e1')
        self.assertEqual(result, 1)

    def test_add_unique_step_description_existing_step2(self):
        """Tests that adding an already existing step description (name identical)
        behaves as expected"""
        result = self.db_test.add_unique_step_description(3, 'a', 'fake_command', 'e1')
        self.assertNotIn(result, self.ids)
        # Check that we generated a log into the DB
        query = f"object_type == 'step_description' AND object_id == {result}"
        result_query = self.db_test.dbsession.query(ObjectActivityLogging).filter(text(query)).all()
        list_dicts = [item.dict() for item in result_query]
        self.assertEqual(len(list_dicts), 1)
        expected_result = {'id': 1, 'object_type': 'step_description',
                           'object_id': result, 'activity': 'creation'}
        # we do this instead of assertListEqual because the id might not be '1' since we already
        # added some logs in the init
        self.assertEqual(list_dicts[0]['object_type'], expected_result['object_type'])
        self.assertEqual(list_dicts[0]['object_id'], expected_result['object_id'])
        self.assertEqual(list_dicts[0]['activity'], expected_result['activity'])

    def test_add_unique_step_description_empty_service_name(self):
        """Tests that adding a step description with an empty service name
        behaves as expected"""
        session_id = 4
        stepd_id = self.db_test.add_unique_step_description(session_id, 'unexisting_name4',
                                                            'fake_command4', '')
        self.assertNotIn(stepd_id, self.ids)
        # Check that we generated service id = 0 in the stored step description
        result = self.db_test.get_step_descriptions_from_session_id(session_id)
        expected_service_id = 0
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['service_id'], expected_service_id)


class TestAddStepDescription(unittest.TestCase):
    """ Test that the function add_step_description behaves as expected.
    """
    def setUp(self):
        """Connect to database for tests.
        """
        self.db_test = WFMDatabase(name=TEST_DATABASE)

        # ids of the already stored services (see __init__.py)
        self.ids = [ 1, 2 ]

    def tearDown(self):
        self.db_test.dbsession.close()

    def test_add_step_description_unexisting_step(self):
        """Tests that adding a not yet existing step description behaves as expected"""
        result = self.db_test.add_step_description(2, 'unexisting_name', 'fake_command', 1)
        self.assertNotIn(result, self.ids)
        # Check that we generated a log into the DB
        query = f"object_type == 'step_description' AND object_id == {result}"
        result_query = self.db_test.dbsession.query(ObjectActivityLogging).filter(text(query)).all()
        list_dicts = [item.dict() for item in result_query]
        self.assertEqual(len(list_dicts), 1)
        expected_result = {'id': 1, 'object_type': 'step_description',
                           'object_id': result, 'activity': 'creation'}
        # we do this instead of assertListEqual because the id might not be '1' since we already
        # added some logs in the init
        self.assertEqual(list_dicts[0]['object_type'], expected_result['object_type'])
        self.assertEqual(list_dicts[0]['object_id'], expected_result['object_id'])
        self.assertEqual(list_dicts[0]['activity'], expected_result['activity'])

    def test_add_step_description_existing_step1(self):
        """Tests that adding an already existing step description (name and session id identical)
        behaves as expected"""
        result = self.db_test.add_step_description(1, 'a1', 'fake_command', 1)
        self.assertNotIn(result, self.ids)
        # Check that we generated a log into the DB
        query = f"object_type == 'step_description' AND object_id == {result}"
        result_query = self.db_test.dbsession.query(ObjectActivityLogging).filter(text(query)).all()
        list_dicts = [item.dict() for item in result_query]
        self.assertEqual(len(list_dicts), 1)
        expected_result = {'id': 1, 'object_type': 'step_description',
                           'object_id': result, 'activity': 'creation'}
        # we do this instead of assertListEqual because the id might not be '1' since we already
        # added some logs in the init
        self.assertEqual(list_dicts[0]['object_type'], expected_result['object_type'])
        self.assertEqual(list_dicts[0]['object_id'], expected_result['object_id'])
        self.assertEqual(list_dicts[0]['activity'], expected_result['activity'])

    def test_add_step_description_existing_step2(self):
        """Tests that adding an already existing step description (name identical)
        behaves as expected"""
        result = self.db_test.add_step_description(2, 'a1', 'fake_command', 1)
        self.assertNotIn(result, self.ids)
        # Check that we generated a log into the DB
        query = f"object_type == 'step_description' AND object_id == {result}"
        result_query = self.db_test.dbsession.query(ObjectActivityLogging).filter(text(query)).all()
        list_dicts = [item.dict() for item in result_query]
        self.assertEqual(len(list_dicts), 1)
        expected_result = {'id': 1, 'object_type': 'step_description',
                           'object_id': result, 'activity': 'creation'}
        # we do this instead of assertListEqual because the id might not be '1' since we already
        # added some logs in the init
        self.assertEqual(list_dicts[0]['object_type'], expected_result['object_type'])
        self.assertEqual(list_dicts[0]['object_id'], expected_result['object_id'])
        self.assertEqual(list_dicts[0]['activity'], expected_result['activity'])


class TestGetAllStepsDescriptions(unittest.TestCase):
    """ Test that the function get_all_steps_descriptions behaves as expected.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        Do this because we need an empty DB for one of the tests.
        """
        self.db_test_priv = WFMDatabase(':memory:')
        Base.metadata.create_all(bind=self.db_test_priv.engine)

    def tearDown(self):
        self.db_test_priv.dbsession.close()

    def test_get_all_steps_descriptions_from_empty_db(self):
        """Tests that getting all steps descriptions behaves as expected when
        the step DB is empty"""
        expected_list = []
        result = self.db_test_priv.get_all_steps_descriptions()
        self.assertListEqual(result, expected_list)

    def test_get_all_steps_descriptions_single_step(self):
        """Tests that getting all steps descriptions behaves as expected when
        the DB contains only one step"""
        # First add a step description (DB is empty)
        stepd1 = StepDescription(session_id=1, name='step1', command='command1', service_id=10)
        self.db_test_priv.dbsession.add(stepd1)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.refresh(stepd1)
        result = self.db_test_priv.get_all_steps_descriptions()
        expected_result = [{'id': stepd1.id, 'name': 'step1', 'session_id': 1,
                            'service_id': stepd1.service_id, 'command': 'command1'}]
        self.assertListEqual(result, expected_result)
        self.db_test_priv.dbsession.delete(stepd1)
        self.db_test_priv.dbsession.commit()

    def test_get_all_steps_descriptions_several_steps(self):
        """Tests that getting all steps descriptions behaves as expected when
        the DB contains several steps"""
        # First add 2 step descriptions (DB is empty)
        stepd1 = StepDescription(session_id=1, name='step1', command='command1', service_id=10)
        stepd2 = StepDescription(session_id=2, name='step2', command='command2', service_id=20)
        self.db_test_priv.dbsession.add(stepd1)
        self.db_test_priv.dbsession.add(stepd2)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.refresh(stepd1)
        self.db_test_priv.dbsession.refresh(stepd2)
        result = self.db_test_priv.get_all_steps_descriptions()
        expected_result = [{'id': stepd1.id, 'name': 'step1', 'session_id': 1,
                            'service_id': stepd1.service_id, 'command': 'command1'},
                           {'id': stepd2.id, 'name': 'step2', 'session_id': 2,
                            'service_id': stepd2.service_id, 'command': 'command2'}]
        self.assertListEqual(result, expected_result)
        self.db_test_priv.dbsession.delete(stepd1)
        self.db_test_priv.dbsession.delete(stepd2)
        self.db_test_priv.dbsession.commit()


class TestGetStepDescriptionInfoFromName(unittest.TestCase):
    """ Test that the function get_step_description_info_from_name behaves as expected.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        Doing this because one of the tests adds elements to the DB.
        """
        self.db_test_priv = WFMDatabase(':memory:')
        Base.metadata.create_all(bind=self.db_test_priv.engine)

        stepd1 = StepDescription(session_id=1, name='step1', command='command1', service_id=10)
        self.db_test_priv.dbsession.add(stepd1)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.refresh(stepd1)
        self.stepd_id1 = stepd1.id
        self.stepd_cmd = stepd1.command
        self.stepd_srvid = stepd1.service_id

    def tearDown(self):
        self.db_test_priv.dbsession.close()

    def test_get_step_description_info_from_name_no_step(self):
        """Tests that getting a step description info by name behaves as expected when
        no step with this name exists in the DB"""
        result = self.db_test_priv.get_step_description_info_from_name('unexisting_name')
        self.assertListEqual(result, [])

    def test_get_step_description_info_from_name_single_step(self):
        """Tests that getting a step description info by name behaves as expected when
        a single step exists with this name in the DB"""
        result = self.db_test_priv.get_step_description_info_from_name('step1')
        expected_result = [{'id': self.stepd_id1, 'name': 'step1', 'session_id': 1,
                            'service_id': self.stepd_srvid, 'command': 'command1'}]
        self.assertListEqual(result, expected_result)

    def test_get_step_description_info_from_name_several_steps(self):
        """Tests that getting a step description info by name behaves as expected when
        several steps with the same name exist in the DB"""
        stepd2 = StepDescription(session_id=2, name='step1', command='command1', service_id=20)
        self.db_test_priv.dbsession.add(stepd2)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.refresh(stepd2)
        result = self.db_test_priv.get_step_description_info_from_name('step1')
        expected_result = [{'id': self.stepd_id1, 'name': 'step1', 'session_id': 1,
                            'service_id': self.stepd_srvid, 'command': self.stepd_cmd},
                           {'id': stepd2.id, 'name': 'step1', 'session_id': 2,
                            'service_id': stepd2.service_id, 'command': 'command1'}]
        self.assertListEqual(result, expected_result)
        self.db_test_priv.dbsession.delete(stepd2)
        self.db_test_priv.dbsession.commit()


class TestGetStepDescriptionsFromSessionId(unittest.TestCase):
    """ Test that the function get_step_descriptions_from_session_id behaves as expected.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        Doing this because one of the tests adds elements to the DB.
        """
        self.db_test_priv = WFMDatabase(':memory:')
        Base.metadata.create_all(bind=self.db_test_priv.engine)

        self.session_id = 3
        stepd1 = StepDescription(session_id=self.session_id, name='step1', command='command1',
                                 service_id=10)
        self.db_test_priv.dbsession.add(stepd1)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.refresh(stepd1)
        self.stepd_id1 = stepd1.id
        self.stepd_cmd = stepd1.command
        self.stepd_srvid = stepd1.service_id

    def tearDown(self):
        self.db_test_priv.dbsession.close()

    def test_get_step_descriptions_from_session_id_no_step(self):
        """Tests that getting a step description info by session id behaves as expected when
        no step description with this session id exists in the DB"""
        result = self.db_test_priv.get_step_descriptions_from_session_id(123)
        self.assertListEqual(result, [])

    def test_get_step_descriptions_from_session_id_single_step(self):
        """Tests that getting a step description info by session id behaves as expected when
        a single step description exists with this session id in the DB"""
        result = self.db_test_priv.get_step_descriptions_from_session_id(self.session_id)
        expected_result = [{'id': self.stepd_id1, 'name': 'step1', 'session_id': self.session_id,
                            'service_id': self.stepd_srvid, 'command': self.stepd_cmd}]
        self.assertListEqual(result, expected_result)

    def test_get_step_descriptions_from_session_id_several_steps(self):
        """Tests that getting a step description info by session id behaves as expected when
        several steps descriptions with the same session id exist in the DB"""
        stepd2 = StepDescription(session_id=self.session_id, name='step2', command='command2',
                                 service_id=20)
        self.db_test_priv.dbsession.add(stepd2)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.refresh(stepd2)
        result = self.db_test_priv.get_step_descriptions_from_session_id(self.session_id)
        expected_result = [{'id': self.stepd_id1, 'name': 'step1', 'session_id': self.session_id,
                            'service_id': self.stepd_srvid, 'command': self.stepd_cmd},
                           {'id': stepd2.id, 'name': stepd2.name, 'session_id': self.session_id,
                            'service_id': stepd2.service_id, 'command': stepd2.command}]
        self.assertListEqual(result, expected_result)
        self.db_test_priv.dbsession.delete(stepd2)
        self.db_test_priv.dbsession.commit()


class TestGetStepDescription(unittest.TestCase):
    """ Test that the function get_step_description behaves as expected.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        Doing this because one of the tests adds elements to the DB.
        """
        self.db_test_priv = WFMDatabase(':memory:')
        Base.metadata.create_all(bind=self.db_test_priv.engine)

        self.stepd1 = StepDescription(session_id=1, name='step1', command='command1', service_id=10)
        self.db_test_priv.dbsession.add(self.stepd1)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.refresh(self.stepd1)

    def tearDown(self):
        self.db_test_priv.dbsession.close()

    def test_get_step_description_no_step(self):
        """Tests that getting a step description info by name behaves as expected when
        no step with this name exists in the DB"""
        result = self.db_test_priv.get_step_description(self.stepd1.session_id + 10,
                                                        'unexisting_name')
        self.assertListEqual(result, [])

    def test_get_step_description_single_step(self):
        """Tests that getting a step description info behaves as expected when
        a single step exists with this step name and this session id in the DB"""
        result = self.db_test_priv.get_step_description(self.stepd1.session_id, self.stepd1.name)
        expected_result = [{'id': self.stepd1.id,
                            'name': self.stepd1.name,
                            'session_id': self.stepd1.session_id,
                            'service_id': self.stepd1.service_id,
                            'command': self.stepd1.command
                           }]
        self.assertListEqual(result, expected_result)

    def test_get_step_description_several_steps(self):
        """Tests that getting a step description info behaves as expected when
        several steps with the same name and session id exist in the DB"""
        stepd2 = StepDescription(session_id=self.stepd1.session_id,
                                name=self.stepd1.name,
                                command='command2', service_id=20)
        self.db_test_priv.dbsession.add(stepd2)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.refresh(stepd2)
        result = self.db_test_priv.get_step_description(self.stepd1.session_id,
                                                        self.stepd1.name)
        expected_result = [{'id': self.stepd1.id, 'name': self.stepd1.name,
                            'session_id': self.stepd1.session_id,
                            'service_id': self.stepd1.service_id, 'command': self.stepd1.command,
                           },
                           {'id': stepd2.id, 'name': self.stepd1.name,
                            'session_id': self.stepd1.session_id,
                            'service_id': stepd2.service_id, 'command': stepd2.command,
                           }]
        self.assertListEqual(result, expected_result)
        self.db_test_priv.dbsession.delete(stepd2)
        self.db_test_priv.dbsession.commit()


class TestGetStepDescriptionFromId(unittest.TestCase):
    """ Test that the function get_step_description_from_id behaves as expected.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        Doing this because one of the tests adds elements to the DB.
        """
        self.db_test_priv = WFMDatabase(':memory:')
        Base.metadata.create_all(bind=self.db_test_priv.engine)

        self.stepd1 = StepDescription(session_id=1, name='step1', command='command1', service_id=10)
        self.db_test_priv.dbsession.add(self.stepd1)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.refresh(self.stepd1)

    def tearDown(self):
        self.db_test_priv.dbsession.close()

    def test_get_step_description_from_id_no_step(self):
        """Tests that getting a step description info by id behaves as expected when
        no step with this id exists in the DB"""
        result = self.db_test_priv.get_step_description_from_id(self.stepd1.session_id + 10)
        self.assertListEqual(result, [])

    def test_get_step_description_from_id_single_step(self):
        """Tests that getting a step description info behaves as expected when
        a single step exists with this step id in the DB"""
        result = self.db_test_priv.get_step_description_from_id(self.stepd1.id)
        expected_result = [{'id': self.stepd1.id,
                            'name': self.stepd1.name,
                            'session_id': self.stepd1.session_id,
                            'service_id': self.stepd1.service_id,
                            'command': self.stepd1.command
                           }]
        self.assertListEqual(result, expected_result)


class TestUpdateStepDescriptionWithStep(unittest.TestCase):
    """ Test that the function update_step_description_with_step behaves as expected.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        Doing this because one of the tests adds elements to the DB
        and all of them delete tests from the DB.
        """
        self.db_test_priv = WFMDatabase(':memory:')
        Base.metadata.create_all(bind=self.db_test_priv.engine)

        self.stepd1 = StepDescription(session_id=1, name='step1', command='command1')
        self.stepd2 = StepDescription(session_id=2, name='step2', command='command2')

        self.step1 = Step(step_description_id=self.stepd1.id, start_time=123, stop_time=123,
                          status='status1', progress='Copying 10%', jobid=10,
                          instance_name='step1_1')
        self.step20 = Step(step_description_id=self.stepd2.id, start_time=123, stop_time=123,
                          status='status20', progress='Copying 20%', jobid=20,
                          instance_name='step2_1')
        self.step21 = Step(step_description_id=self.stepd2.id, start_time=123, stop_time=123,
                          status='status21', progress='Copying 21%', jobid=21,
                          instance_name='step2_2')

        # intentionally leaving the list empty
        self.stepd1.steps = []
        self.stepd2.steps = [self.step20]

        self.db_test_priv.dbsession.add(self.stepd1)
        self.db_test_priv.dbsession.add(self.stepd2)
        self.db_test_priv.dbsession.add(self.step1)
        self.db_test_priv.dbsession.add(self.step20)
        self.db_test_priv.dbsession.add(self.step21)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.refresh(self.stepd1)
        self.db_test_priv.dbsession.refresh(self.stepd2)
        self.db_test_priv.dbsession.refresh(self.step1)
        self.db_test_priv.dbsession.refresh(self.step20)
        self.db_test_priv.dbsession.refresh(self.step21)

    def tearDown(self):
        self.db_test_priv.dbsession.delete(self.stepd1)
        self.db_test_priv.dbsession.delete(self.stepd2)
        self.db_test_priv.dbsession.delete(self.step1)
        self.db_test_priv.dbsession.delete(self.step20)
        self.db_test_priv.dbsession.delete(self.step21)
        self.db_test_priv.dbsession.close()

    def test_update_step_description_with_step_none(self):
        """Tests that updating a step description's step list behaves as expected when
        no step description nor step exist with these ids in the DB"""
        result = self.db_test_priv.update_step_description_with_step(123, 123)
        expected_result = 1
        # Check that we didn't generate any log into the DB
        self.assertEqual(result, expected_result)

    def test_update_step_description_with_step_no_step_description(self):
        """Tests that updating a step description's step list behaves as expected when
        no step description exists with this id in the DB"""
        result = self.db_test_priv.update_step_description_with_step(123, self.step20.id)
        expected_result = 1
        # Check that we didn't generate any log into the DB
        self.assertEqual(result, expected_result)

    def test_update_step_description_with_step_no_step(self):
        """Tests that updating a step description's step list behaves as expected when
        no step exists with this id in the DB"""
        result = self.db_test_priv.update_step_description_with_step(self.stepd1.id, 123)
        expected_result = 2
        # Check that we didn't generate any log into the DB
        self.assertEqual(result, expected_result)

    def test_update_step_description_with_step_empty_step_list(self):
        """Tests that updating a step description's step list behaves as expected when
        a step description exists with this id in the DB and its step list is empty"""
        result = self.db_test_priv.update_step_description_with_step(self.stepd1.id, self.step1.id)
        self.assertEqual(result, 0)
        expected_result = [self.step1]
        result = self.db_test_priv.get_objs_query(StepDescription, f"id == '{self.stepd1.id}'")
        self.assertListEqual(result[0].steps, expected_result)

    def test_update_step_description_with_step_single_elem_step_list(self):
        """Tests that updating a step description's step list behaves as expected when
        a step description exists with this id in the DB and its step list is not empty"""
        result = self.db_test_priv.update_step_description_with_step(self.stepd2.id, self.step21.id)
        self.assertEqual(result, 0)
        expected_result = [self.step20, self.step21]
        result = self.db_test_priv.get_objs_query(StepDescription, f"id == '{self.stepd2.id}'")
        self.assertListEqual(result[0].steps, expected_result)


class TestDeleteStepDescription(unittest.TestCase):
    """ Test that the function delete_step_description behaves as expected.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        Doing this because one of the tests adds elements to the DB
        and all of them delete tests from the DB.
        """
        self.db_test_priv = WFMDatabase(':memory:')
        Base.metadata.create_all(bind=self.db_test_priv.engine)

        stepd1 = StepDescription(session_id=1, name='step1', command='command1')
        stepd2 = StepDescription(session_id=2, name='step2', command='command2')
        self.db_test_priv.dbsession.add(stepd1)
        self.db_test_priv.dbsession.add(stepd2)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.refresh(stepd1)
        self.db_test_priv.dbsession.refresh(stepd2)
        self.stepd_id1 = stepd1.id

    def tearDown(self):
        self.db_test_priv.dbsession.close()

    def test_delete_step_description_no_step(self):
        """Tests that deleting a step description behaves as expected when
        no step description exists with this id in the DB"""
        self.db_test_priv.delete_step_description(123)
        # Check that we didn't generate any log into the DB
        query = "object_type == 'step_description'"
        result = self.db_test_priv.dbsession.query(ObjectActivityLogging).filter(text(query)).all()
        self.assertListEqual(result, [])

    def test_delete_step_description_single_step(self):
        """Tests that deleting a step description behaves as expected when
        a step description exists with this id in the DB"""
        self.db_test_priv.delete_step_description(self.stepd_id1)
        result = self.db_test_priv.get_step_description_info_from_name('step1')
        self.assertListEqual(result, [])
        # Check that we generated a log into the DB
        query = "object_type == 'step_description'"
        result = self.db_test_priv.dbsession.query(ObjectActivityLogging).filter(text(query)).all()
        expected_result = {'id': 1, 'object_type': 'step_description',
                           'object_id': self.stepd_id1, 'activity': 'removal'}
        list_dicts = [item.dict() for item in result]
        # we do this instead of assertListEqual because the id might not be '1' since we already
        # added some logs in the init
        self.assertEqual(list_dicts[0]['object_type'], expected_result['object_type'])
        self.assertEqual(list_dicts[0]['object_id'], expected_result['object_id'])
        self.assertEqual(list_dicts[0]['activity'], expected_result['activity'])


class TestAddStep(unittest.TestCase):
    """ Test that the function add_step behaves as expected.
    """
    def setUp(self):
        """Connect to database for tests.
        """
        self.db_test = WFMDatabase(name=TEST_DATABASE)

        # ids of the already stored steps (see __init__.py)
        self.step_ids = [ 1, 2, 3 ]

    def tearDown(self):
        self.db_test.dbsession.close()

    def test_add_step_unexisting_step(self):
        """Tests that adding a not existing description step behaves as expected"""
        with self.assertRaises(NoDocumentError):
            self.db_test.add_step(step_descrid=40, step_status='status4',
                                  step_progress='progress', step_command='command4')

    def test_add_step_existing_step(self):
        """Tests that adding an already existing step (step description
        ids identical) behaves as expected"""
        step_id, step_name = self.db_test.add_step(step_descrid=1, step_status='status5',
                                                   step_progress='progress5', step_command='command5')
        self.assertNotIn(step_id, self.step_ids)
        self.assertEqual(step_name, "user-s1-a_3")
        # Check that we generated a log into the DB
        query = f"object_type == 'step' AND object_id == {step_id}"
        result_query = self.db_test.dbsession.query(ObjectActivityLogging).filter(text(query)).all()
        list_dicts = [item.dict() for item in result_query]
        self.assertEqual(len(list_dicts), 1)
        expected_result = {'id': 1, 'object_type': 'step',
                           'object_id': step_id, 'activity': 'creation'}
        # we do this instead of assertListEqual because the id might not be '1' since we already
        # added some logs in the init
        self.assertEqual(list_dicts[0]['object_type'], expected_result['object_type'])
        self.assertEqual(list_dicts[0]['object_id'], expected_result['object_id'])
        self.assertEqual(list_dicts[0]['activity'], expected_result['activity'])
        # delete added step
        self.db_test.delete_step(step_id=step_id)

class TestUniqueStepInstanceName(unittest.TestCase):
    """ Test that the function unique_step_instance_name behaves as expected.
    """
    def setUp(self):
        """Connect to database for tests.
        """
        self.db_test = WFMDatabase(name=TEST_DATABASE)

    def tearDown(self):
        self.db_test.dbsession.close()

    def test_unique_step_instance_name_no_step_descr(self):
        """Tests a call with a not existing description id behaves as expected"""
        with self.assertRaises(NoDocumentError):
            self.db_test.unique_step_instance_name(100)

    def test_unique_step_instance_name(self):
        """Tests that the function return the expected step name """
        result = self.db_test.unique_step_instance_name(1)
        self.assertEqual(result, "user-s1-a_3")


class TestGetAllSteps(unittest.TestCase):
    """ Test that the function get_all_steps behaves as expected.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        Do this because we need an empty DB for one of the tests.
        """
        self.db_test_priv = WFMDatabase(':memory:')
        Base.metadata.create_all(bind=self.db_test_priv.engine)

    def tearDown(self):
        self.db_test_priv.dbsession.close()

    def test_get_all_steps_from_empty_db(self):
        """Tests that getting all steps behaves as expected when
        the step DB is empty"""
        expected_list = []
        result = self.db_test_priv.get_all_steps()
        self.assertListEqual(result, expected_list)

    def test_get_all_steps_single_step(self):
        """Tests that getting all steps behaves as expected when
        the DB contains only one step"""
        # First add a step (DB is empty)
        step1 = Step(step_description_id=1, start_time=123, stop_time=123,
                     status='status1', progress='progress1', jobid=1, instance_name='step1_1')
        self.db_test_priv.dbsession.add(step1)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.refresh(step1)
        result = self.db_test_priv.get_all_steps()
        expected_result = [{'id': step1.id,'instance_name': step1.instance_name,
                            'status': step1.status,
                            'progress': step1.progress,
                            'jobid': step1.jobid, 'step_description_id': step1.step_description_id}]
        self.assertListEqual(result, expected_result)
        self.db_test_priv.dbsession.delete(step1)
        self.db_test_priv.dbsession.commit()

    def test_get_all_steps_several_steps(self):
        """Tests that getting all steps behaves as expected when
        the DB contains several steps"""
        # First add 2 step  (DB is empty)
        step1 = Step(step_description_id=1, start_time=123, stop_time=123,
                     status='status1', progress='progress1', jobid=1, instance_name='step1_1')
        step2 = Step(step_description_id=2, start_time=123, stop_time=123,
                     status='status2', progress='progress2', jobid=2, instance_name='step2_1')
        self.db_test_priv.dbsession.add(step1)
        self.db_test_priv.dbsession.add(step2)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.refresh(step1)
        self.db_test_priv.dbsession.refresh(step2)
        result = self.db_test_priv.get_all_steps()
        expected_result = [{'id': step1.id, 'instance_name': step1.instance_name,
                            'status': step1.status,
                            'progress': step1.progress,
                            'jobid': step1.jobid, 'step_description_id': step1.step_description_id},
                           {'id': step2.id, 'instance_name': step2.instance_name,
                            'status': step2.status,
                            'progress': step2.progress,
                            'jobid': step2.jobid, 'step_description_id': step2.step_description_id}]
        self.assertListEqual(result, expected_result)
        self.db_test_priv.dbsession.delete(step1)
        self.db_test_priv.dbsession.delete(step2)
        self.db_test_priv.dbsession.commit()


class TestGetStepsInfoFromSessionId(unittest.TestCase):
    """ Test that the function get_steps_info_from_session_id behaves as expected.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        Doing this because one of the tests adds elements to the DB.
        """
        self.db_test_priv = WFMDatabase(':memory:')
        Base.metadata.create_all(bind=self.db_test_priv.engine)

        stepd1 = StepDescription(session_id=1, name='step1', command='command1')
        self.step1 = Step(step_description_id=stepd1.id, start_time=123, stop_time=123,
                          status='status1', progress='progress1', jobid=3, instance_name='step1_1')
        stepd1.steps = [ self.step1 ]
        self.db_test_priv.dbsession.add(stepd1)
        self.db_test_priv.dbsession.add(self.step1)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.refresh(stepd1)
        self.db_test_priv.dbsession.refresh(self.step1)
        self.step_id1 = self.step1.id
        self.ses_id1 = stepd1.session_id
        self.stepd1 = stepd1

    def tearDown(self):
        self.db_test_priv.dbsession.delete(self.step1)
        self.db_test_priv.dbsession.delete(self.stepd1)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.close()

    def test_get_steps_info_from_session_id_no_step(self):
        """Tests that getting steps info by session id behaves as expected when
        no step with this session id exists in the DB"""
        result = self.db_test_priv.get_steps_info_from_session_id(123)
        self.assertListEqual(result, [])

    def test_get_steps_info_from_session_id_single_step(self):
        """Tests that getting steps info by session id behaves as expected when
        a single step exists with this session id in the DB"""
        result = self.db_test_priv.get_steps_info_from_session_id(self.ses_id1)
        expected_result = [{'id': self.step_id1, 'status': 'status1',
                            'progress':self.step1.progress, 'jobid': 3,
                            'step_description_id': self.stepd1.id,
                            'instance_name': self.step1.instance_name}]
        self.assertListEqual(result, expected_result)

    def test_get_steps_info_from_session_id_several_steps(self):
        """Tests that getting steps info by session id behaves as expected when
        several steps with the same session id exist in the DB"""
        # Add another step with the same session id to the DB
        stepd2 = StepDescription(session_id=self.ses_id1, name='step2', command='command2')
        step2 = Step(step_description_id=stepd2.id, instance_name='step2_1',
                     start_time=456, stop_time=456,
                     status='status2', progress='progress2', jobid=30)
        stepd2.steps = [ step2 ]
        self.db_test_priv.dbsession.add(stepd2)
        self.db_test_priv.dbsession.add(step2)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.refresh(stepd2)
        self.db_test_priv.dbsession.refresh(step2)
        result = self.db_test_priv.get_steps_info_from_session_id(self.ses_id1)
        expected_result = [{'id': self.step_id1, 'instance_name': self.step1.instance_name,
                            'status': 'status1', 'progress': self.step1.progress,
                            'jobid': 3, 'step_description_id': self.step1.step_description_id},
                           {'id': step2.id, 'instance_name': step2.instance_name,
                            'status': 'status2', 'progress': step2.progress, 'jobid': 30,
                            'step_description_id': stepd2.id}]
        self.assertListEqual(result, expected_result)
        self.db_test_priv.dbsession.delete(step2)
        self.db_test_priv.dbsession.delete(stepd2)
        self.db_test_priv.dbsession.commit()


class TestGetStepsInfoFromStepDescriptionId(unittest.TestCase):
    """ Test that the function get_steps_info_from_step_description_id behaves as expected.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        Doing this because one of the tests adds elements to the DB.
        """
        self.db_test_priv = WFMDatabase(':memory:')
        Base.metadata.create_all(bind=self.db_test_priv.engine)

        self.step1 = Step(step_description_id=2, start_time=123, stop_time=123,
                     status='status1', progress='progress1', jobid=3, instance_name='step_1')
        self.db_test_priv.dbsession.add(self.step1)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.refresh(self.step1)
        self.step_id1 = self.step1.id
        self.step_descr_id1 = self.step1.step_description_id

    def tearDown(self):
        self.db_test_priv.dbsession.delete(self.step1)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.close()

    def test_get_steps_info_from_step_description_no_step(self):
        """Tests that getting steps info by step description id behaves as expected when
        no step with this step description id exists in the DB"""
        result = self.db_test_priv.get_steps_info_from_step_description_id(123)
        self.assertListEqual(result, [])

    def test_get_steps_info_from_step_description_single_step(self):
        """Tests that getting steps info by step description id behaves as expected when
        a single step exists with this step description id in the DB"""
        result = self.db_test_priv.get_steps_info_from_step_description_id(self.step_descr_id1)
        expected_result = [{'id': self.step_id1, 'status': 'status1',
                            'progress': self.step1.progress, 'jobid': 3,
                            'step_description_id': self.step_descr_id1,
                            'instance_name': self.step1.instance_name}]
        self.assertListEqual(result, expected_result)

    def test_get_steps_info_from_step_description_several_steps(self):
        """Tests that getting steps info by step description id behaves as expected when
        several steps with the same step description id exist in the DB"""
        # Add another step with the same step_description id to the DB
        step2 = Step(step_description_id=self.step_descr_id1, instance_name= 'step_2',
                     start_time=456, stop_time=456,
                     status='status2', progress='progress2', jobid=30)
        self.db_test_priv.dbsession.add(step2)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.refresh(step2)
        result = self.db_test_priv.get_steps_info_from_step_description_id(self.step_descr_id1)
        expected_result = [{'id': self.step_id1, 'status': 'status1',
                            'progress': self.step1.progress, 'jobid': 3,
                            'step_description_id': self.step_descr_id1,
                            'instance_name': self.step1.instance_name},
                           {'id': step2.id, 'status': 'status2',
                            'progress': step2.progress, 'jobid': 30,
                            'step_description_id': self.step_descr_id1,
                            'instance_name': step2.instance_name}]
        self.assertListEqual(result, expected_result)
        self.db_test_priv.dbsession.delete(step2)
        self.db_test_priv.dbsession.commit()


class TestGetStepsInfoFromStepDescriptionName(unittest.TestCase):
    """ Test that the function get_steps_info_from_step_description_name behaves as expected.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        Doing this because one of the tests adds elements to the DB.
        """
        self.db_test = WFMDatabase(name=TEST_DATABASE)

    def tearDown(self):
        self.db_test.dbsession.close()

    def test_get_steps_info_from_step_description_name_no_step(self):
        """Tests that getting steps info by step description name behaves as expected when
        no step with this step description name exists in the DB"""
        with self.assertRaises(NoDocumentError):
            self.db_test.get_steps_info_from_step_description_name(session_id=1,
                                                                   stepd_name="no_name")

    def test_get_steps_info_from_step_description_name_single_step(self):
        """Tests that getting steps info by step description name behaves as expected when
        a single step exists with this step description name in the DB"""
        result = self.db_test.get_steps_info_from_step_description_name(session_id=2,
                                                                        stepd_name="a")
        expected_result = [{'id': 3, 'status': 'status20', 'progress': "Copying 20%",
                            'step_description_id': 2, 'jobid': 2000,
                            'instance_name': "s2_a_1"}]
        self.assertListEqual(result, expected_result)

    def test_get_steps_info_from_step_description_name_several_steps(self):
        """Tests that getting steps info by step description name behaves as expected when
        several steps with the same step description name exist in the DB"""
        result = self.db_test.get_steps_info_from_step_description_name(session_id=1,
                                                                        stepd_name="a")
        expected_result = [{'id': 1, 'status': 'status10','progress': "Preparing",
                            'step_description_id': 1, 'jobid': None,
                            'instance_name': "s1_a_1"},
                           {'id': 2, 'status': 'status11','progress': "Syncing",
                            'step_description_id': 1, 'jobid': 1100,
                            'instance_name': "s1_a_2"}]
        self.assertListEqual(result, expected_result)


class TestGetStepsInfoFromJobid(unittest.TestCase):
    """ Test that the function get_steps_info_from_jobid behaves as expected.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        Doing this because one of the tests adds elements to the DB.
        """
        self.db_test = WFMDatabase(name=TEST_DATABASE)

    def tearDown(self):
        self.db_test.dbsession.close()

    def test_get_steps_info_from_jobid_no_step(self):
        """Tests that getting steps info by jobid behaves as expected when
        no step with this jobid exists in the DB"""
        result = self.db_test.get_steps_info_from_jobid(9235)
        expected_result = []
        self.assertListEqual(result, expected_result)

    def test_get_steps_info_from_jobid_single_step(self):
        """Tests that getting steps info by jobid behaves as expected when
        a single step exists with this jobid in the DB"""
        result = self.db_test.get_steps_info_from_jobid(2000)
        expected_result = [{'id': 3, 'status': 'status20', 'progress': "Copying 20%",
                            'step_description_id': 2, 'jobid': 2000,
                            'instance_name': "s2_a_1"}]
        self.assertListEqual(result, expected_result)

    def test_get_steps_info_from_jobid_several_steps(self):
        """Tests that getting steps info by jobid behaves as expected when
        a several steps exist with this jobid in the DB"""
        # Add a step with an already existing jobid
        step_a21 = Step(step_description_id=2, instance_name="s2_a_2",
                        start_time=21, stop_time=21, status="status21",
                        jobid=2000, progress="progress21")
        self.db_test.dbsession.add(step_a21)
        self.db_test.dbsession.commit()
        self.db_test.dbsession.refresh(step_a21)
        result = self.db_test.get_steps_info_from_jobid(2000)
        expected_result = [{'id': 3, 'status': 'status20', 'progress': "Copying 20%",
                            'step_description_id': 2, 'jobid': 2000,
                            'instance_name': "s2_a_1"},
                           {'id': 4, 'status': 'status21', 'progress': "progress21",
                            'step_description_id': 2, 'jobid': 2000,
                            'instance_name': "s2_a_2"}]
        self.assertListEqual(result, expected_result)
        self.db_test.dbsession.delete(step_a21)
        self.db_test.dbsession.commit()


class TestDeleteStep(unittest.TestCase):
    """ Test that the function delete_step behaves as expected.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        Doing this because one of the tests adds elements to the DB
        and all of them delete tests from the DB.
        """
        self.db_test_priv = WFMDatabase(':memory:')
        Base.metadata.create_all(bind=self.db_test_priv.engine)

        self.session_id = 1
        stepd1 = StepDescription(session_id=self.session_id, name='step1', command='command1')
        self.step1 = Step(step_description_id=1, start_time=123, stop_time=123,
                          status='status1', progress='progress1', jobid=1, instance_name='step1_1')
        stepd2 = StepDescription(session_id=self.session_id, name='step2', command='command2')
        self.step2 = Step(step_description_id=2, start_time=123, stop_time=123,
                          status='status2', progress='progress2', jobid=2, instance_name="step2_1")
        self.db_test_priv.dbsession.add(stepd1)
        self.db_test_priv.dbsession.add(stepd2)
        self.db_test_priv.dbsession.add(self.step1)
        self.db_test_priv.dbsession.add(self.step2)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.refresh(stepd1)
        self.db_test_priv.dbsession.refresh(stepd2)
        self.db_test_priv.dbsession.refresh(self.step1)
        self.db_test_priv.dbsession.refresh(self.step2)

    def tearDown(self):
        self.db_test_priv.dbsession.close()

    def test_delete_step_no_step(self):
        """Tests that deleting a step behaves as expected when
        no step exists with this id in the DB"""
        self.db_test_priv.delete_step(123)
        # Check that we generated a log into the DB
        result = self.db_test_priv.dbsession.query(ObjectActivityLogging).filter(text(f"object_type == 'step'")).all()
        self.assertListEqual(result, [])

    def test_delete_step_step_exists(self):
        """Tests that deleting a step behaves as expected when
        a step exists with this id in the DB"""
        self.db_test_priv.delete_step(self.step1.id)
        result = self.db_test_priv.get_steps_info_from_session_id(self.session_id)
        expected_result = [{'id': self.step2.id, 'jobid': 2, 'status': 'status2',
                            'progress': self.step2.progress,
                            'step_description_id': 2, 'instance_name': self.step2.instance_name}]
        self.assertListEqual(result, expected_result)
        # Check that we generated a log into the DB
        filter_txt = text(f"object_type == 'step'")
        result = self.db_test_priv.dbsession.query(ObjectActivityLogging).filter(filter_txt).all()
        list_dicts = [item.dict() for item in result]
        expected_result = {'id': 1, 'object_type': 'step',
                           'object_id': self.step1.id, 'activity': 'removal'}
        # we do this instead of assertListEqual because the id might not be '1' since we already
        # added some logs in the init
        self.assertEqual(list_dicts[0]['object_type'], expected_result['object_type'])
        self.assertEqual(list_dicts[0]['object_id'], expected_result['object_id'])
        self.assertEqual(list_dicts[0]['activity'], expected_result['activity'])


class TestUpdateStepStatus(unittest.TestCase):
    """ Test that the function update_step_status behaves as expected.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        Doing this because one of the tests adds elements to the DB.
        """
        self.db_test_priv = WFMDatabase(':memory:')
        Base.metadata.create_all(bind=self.db_test_priv.engine)

        self.step1 = Step(step_description_id=1, start_time=123, stop_time=123,
                          status='status1', progress='progress1', jobid=1, instance_name='step_1')
        self.db_test_priv.dbsession.add(self.step1)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.refresh(self.step1)

    def tearDown(self):
        self.db_test_priv.dbsession.delete(self.step1)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.close()

    def test_update_step_status_no_step(self):
        """Tests that updating a step status behaves as expected when
        no step exists with this id in the DB"""
        new_status = 'status10'
        unknown_step_id = 1234
        self.db_test_priv.update_step_status(unknown_step_id, new_status)
        result = self.db_test_priv.get_steps_info_from_step_description_id(self.step1.step_description_id)
        expected_result = [{'id': self.step1.id,
                            'jobid': self.step1.jobid,
                            'instance_name': self.step1.instance_name,
                            'status': self.step1.status,
                            'progress': self.step1.progress,
                            'step_description_id': self.step1.step_description_id}]
        self.assertListEqual(result, expected_result)

    def test_update_step_status_single_step(self):
        """Tests that updating a step status behaves as expected when
        a single step exists with this description id in the DB"""
        new_status = 'status10'
        self.db_test_priv.update_step_status(self.step1.id, new_status)
        result = self.db_test_priv.get_steps_info_from_step_description_id(self.step1.step_description_id)
        expected_result = [{'id': self.step1.id,
                            'jobid': self.step1.jobid,
                            'instance_name': self.step1.instance_name,
                            'status': new_status,
                            'progress': self.step1.progress,
                            'step_description_id': self.step1.step_description_id}]
        self.assertListEqual(result, expected_result)


class TestUpdateStepJobid(unittest.TestCase):
    """ Test that the function update_step_jobid behaves as expected.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        Doing this because one of the tests adds elements to the DB.
        """
        self.db_test_priv = WFMDatabase(':memory:')
        Base.metadata.create_all(bind=self.db_test_priv.engine)

        self.step1 = Step(step_description_id=1, start_time=123, stop_time=123,
                          status='status1', progress='progress1', jobid=1, instance_name='step_1')
        self.db_test_priv.dbsession.add(self.step1)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.refresh(self.step1)

    def tearDown(self):
        self.db_test_priv.dbsession.delete(self.step1)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.close()

    def test_update_step_jobid_no_step(self):
        """Tests that updating a step jobid behaves as expected when
        no step exists with this id in the DB"""
        new_jobid = 100
        unknown_step_id = 1234
        self.db_test_priv.update_step_jobid(unknown_step_id, new_jobid)
        result = self.db_test_priv.get_steps_info_from_step_description_id(self.step1.step_description_id)
        expected_result = [{'id': self.step1.id,
                            'jobid': self.step1.jobid,
                            'instance_name': self.step1.instance_name,
                            'status': self.step1.status,
                            'progress': self.step1.progress,
                            'step_description_id': self.step1.step_description_id}]
        self.assertListEqual(result, expected_result)

    def test_update_step_jobid_single_step(self):
        """Tests that updating a step jobid behaves as expected when
        a single step exists with this description id in the DB"""
        new_jobid = 100
        self.db_test_priv.update_step_jobid(self.step1.id, new_jobid)
        result = self.db_test_priv.get_steps_info_from_step_description_id(self.step1.step_description_id)
        expected_result = [{'id': self.step1.id,
                            'jobid': new_jobid,
                            'instance_name': self.step1.instance_name,
                            'status': self.step1.status,
                            'progress': self.step1.progress,
                            'step_description_id': self.step1.step_description_id}]
        self.assertListEqual(result, expected_result)


class TestUpdateStepProgress(unittest.TestCase):
    """ Test that the function update_step_progress behaves as expected.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        Doing this because one of the tests adds elements to the DB.
        """
        self.db_test_priv = WFMDatabase(':memory:')
        Base.metadata.create_all(bind=self.db_test_priv.engine)

        self.step1 = Step(step_description_id=1, start_time=123, stop_time=123,
                          status='status1', progress='progress1', jobid=1, instance_name='step_1')
        self.db_test_priv.dbsession.add(self.step1)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.refresh(self.step1)

    def tearDown(self):
        self.db_test_priv.dbsession.delete(self.step1)
        self.db_test_priv.dbsession.commit()
        self.db_test_priv.dbsession.close()

    def test_update_step_progress_no_step(self):
        """Tests that updating a step progress behaves as expected when
        no step exists with this id in the DB"""
        new_progress = 90
        unknown_step_id = 1234
        self.db_test_priv.update_step_progress(unknown_step_id, new_progress)
        result = self.db_test_priv.get_steps_info_from_step_description_id(self.step1.step_description_id)
        expected_result = [{'id': self.step1.id,
                            'jobid': self.step1.jobid,
                            'instance_name': self.step1.instance_name,
                            'status': self.step1.status,
                            'progress': self.step1.progress,
                            'step_description_id': self.step1.step_description_id}]
        self.assertListEqual(result, expected_result)

    def test_update_step_progress_single_step(self):
        """Tests that updating a step progress behaves as expected when
        a single step exists with this description id in the DB"""
        new_progress = 'progress90'
        self.db_test_priv.update_step_progress(self.step1.id, new_progress)
        result = self.db_test_priv.get_steps_info_from_step_description_id(self.step1.step_description_id)
        expected_result = [{'id': self.step1.id,
                            'jobid': self.step1.jobid,
                            'instance_name': self.step1.instance_name,
                            'status': self.step1.status,
                            'progress': new_progress,
                            'step_description_id': self.step1.step_description_id}]
        self.assertListEqual(result, expected_result)


class TestLogObjectActivity(unittest.TestCase):
    """ Test that the functions log_<object>_creation / log_<object>_removal behave
    as expected.
    """
    def setUp(self):
        """Set up the tests by creating a mock WFM database.
        Doing this because one of the tests adds elements to the DB.
        """
        self.db_test_priv = WFMDatabase(':memory:')
        Base.metadata.create_all(bind=self.db_test_priv.engine)

    def tearDown(self):
        self.db_test_priv.dbsession.close()

    def test_log_session_creation(self):
        """Tests that logging session creation behaves as expected
        """
        object_id = 123
        res1 = self.db_test_priv.log_session_creation(session_id=object_id)
        self.assertEqual(res1, 1)
        result = self.db_test_priv.dbsession.query(ObjectActivityLogging).filter(text(f"object_id == {object_id}")).all()
        list_dicts = [item.dict() for item in result]
        expected_result = {'id': 1, 'object_type': 'session',
                           'object_id': object_id, 'activity': 'creation'}
        # we do this instead of assertListEqual because the id might not be '1' since we already
        # added some logs in the init
        self.assertEqual(list_dicts[0]['object_type'], expected_result['object_type'])
        self.assertEqual(list_dicts[0]['object_id'], expected_result['object_id'])
        self.assertEqual(list_dicts[0]['activity'], expected_result['activity'])
        for item in result:
            self.db_test_priv.dbsession.delete(item)
        self.db_test_priv.dbsession.commit()

    def test_log_session_removal(self):
        """Tests that logging session creation removal as expected
        """
        object_id = 123
        res1 = self.db_test_priv.log_session_removal(session_id=object_id)
        self.assertEqual(res1, 1)
        result = self.db_test_priv.dbsession.query(ObjectActivityLogging).filter(text(f"object_id == {object_id}")).all()
        list_dicts = [item.dict() for item in result]
        expected_result = {'id': 1, 'object_type': 'session',
                           'object_id': object_id, 'activity': 'removal'}
        # we do this instead of assertListEqual because the id might not be '1' since we already
        # added some logs in the init
        self.assertEqual(list_dicts[0]['object_type'], expected_result['object_type'])
        self.assertEqual(list_dicts[0]['object_id'], expected_result['object_id'])
        self.assertEqual(list_dicts[0]['activity'], expected_result['activity'])
        for item in result:
            self.db_test_priv.dbsession.delete(item)
        self.db_test_priv.dbsession.commit()

    def test_log_service_creation(self):
        """Tests that logging service creation behaves as expected
        """
        object_id = 123
        res1 = self.db_test_priv.log_service_creation(service_id=object_id)
        self.assertEqual(res1, 1)
        result = self.db_test_priv.dbsession.query(ObjectActivityLogging).filter(text(f"object_id == {object_id}")).all()
        list_dicts = [item.dict() for item in result]
        expected_result = {'id': 1, 'object_type': 'service',
                           'object_id': object_id, 'activity': 'creation'}
        # we do this instead of assertListEqual because the id might not be '1' since we already
        # added some logs in the init
        self.assertEqual(list_dicts[0]['object_type'], expected_result['object_type'])
        self.assertEqual(list_dicts[0]['object_id'], expected_result['object_id'])
        self.assertEqual(list_dicts[0]['activity'], expected_result['activity'])
        for item in result:
            self.db_test_priv.dbsession.delete(item)
        self.db_test_priv.dbsession.commit()

    def test_log_service_removal(self):
        """Tests that logging service creation removal as expected
        """
        object_id = 123
        res1 = self.db_test_priv.log_service_removal(service_id=object_id)
        self.assertEqual(res1, 1)
        result = self.db_test_priv.dbsession.query(ObjectActivityLogging).filter(text(f"object_id == {object_id}")).all()
        list_dicts = [item.dict() for item in result]
        expected_result = {'id': 1, 'object_type': 'service',
                           'object_id': object_id, 'activity': 'removal'}
        # we do this instead of assertListEqual because the id might not be '1' since we already
        # added some logs in the init
        self.assertEqual(list_dicts[0]['object_type'], expected_result['object_type'])
        self.assertEqual(list_dicts[0]['object_id'], expected_result['object_id'])
        self.assertEqual(list_dicts[0]['activity'], expected_result['activity'])
        for item in result:
            self.db_test_priv.dbsession.delete(item)
        self.db_test_priv.dbsession.commit()

    def test_log_step_description_creation(self):
        """Tests that logging step description creation behaves as expected
        """
        object_id = 123
        res1 = self.db_test_priv.log_step_description_creation(step_description_id=object_id)
        self.assertEqual(res1, 1)
        result = self.db_test_priv.dbsession.query(ObjectActivityLogging).filter(text(f"object_id == {object_id}")).all()
        list_dicts = [item.dict() for item in result]
        expected_result = {'id': 1, 'object_type': 'step_description',
                           'object_id': object_id, 'activity': 'creation'}
        # we do this instead of assertListEqual because the id might not be '1' since we already
        # added some logs in the init
        self.assertEqual(list_dicts[0]['object_type'], expected_result['object_type'])
        self.assertEqual(list_dicts[0]['object_id'], expected_result['object_id'])
        self.assertEqual(list_dicts[0]['activity'], expected_result['activity'])
        for item in result:
            self.db_test_priv.dbsession.delete(item)
        self.db_test_priv.dbsession.commit()

    def test_log_step_description_removal(self):
        """Tests that logging step description creation removal as expected
        """
        object_id = 123
        res1 = self.db_test_priv.log_step_description_removal(step_description_id=object_id)
        self.assertEqual(res1, 1)
        result = self.db_test_priv.dbsession.query(ObjectActivityLogging).filter(text(f"object_id == {object_id}")).all()
        list_dicts = [item.dict() for item in result]
        expected_result = {'id': 1, 'object_type': 'step_description',
                           'object_id': object_id, 'activity': 'removal'}
        # we do this instead of assertListEqual because the id might not be '1' since we already
        # added some logs in the init
        self.assertEqual(list_dicts[0]['object_type'], expected_result['object_type'])
        self.assertEqual(list_dicts[0]['object_id'], expected_result['object_id'])
        self.assertEqual(list_dicts[0]['activity'], expected_result['activity'])
        for item in result:
            self.db_test_priv.dbsession.delete(item)
        self.db_test_priv.dbsession.commit()

    def test_log_step_creation(self):
        """Tests that logging step creation behaves as expected
        """
        object_id = 123
        res1 = self.db_test_priv.log_step_creation(step_id=object_id)
        self.assertEqual(res1, 1)
        result = self.db_test_priv.dbsession.query(ObjectActivityLogging).filter(text(f"object_id == {object_id}")).all()
        list_dicts = [item.dict() for item in result]
        expected_result = {'id': 1, 'object_type': 'step',
                           'object_id': object_id, 'activity': 'creation'}
        # we do this instead of assertListEqual because the id might not be '1' since we already
        # added some logs in the init
        self.assertEqual(list_dicts[0]['object_type'], expected_result['object_type'])
        self.assertEqual(list_dicts[0]['object_id'], expected_result['object_id'])
        self.assertEqual(list_dicts[0]['activity'], expected_result['activity'])
        for item in result:
            self.db_test_priv.dbsession.delete(item)
        self.db_test_priv.dbsession.commit()

    def test_log_step_removal(self):
        """Tests that logging step creation removal as expected
        """
        object_id = 123
        res1 = self.db_test_priv.log_step_removal(step_id=object_id)
        self.assertEqual(res1, 1)
        result = self.db_test_priv.dbsession.query(ObjectActivityLogging).filter(text(f"object_id == {object_id}")).all()
        list_dicts = [item.dict() for item in result]
        expected_result = {'id': 1, 'object_type': 'step',
                           'object_id': object_id, 'activity': 'removal'}
        # we do this instead of assertListEqual because the id might not be '1' since we already
        # added some logs in the init
        self.assertEqual(list_dicts[0]['object_type'], expected_result['object_type'])
        self.assertEqual(list_dicts[0]['object_id'], expected_result['object_id'])
        self.assertEqual(list_dicts[0]['activity'], expected_result['activity'])
        for item in result:
            self.db_test_priv.dbsession.delete(item)
        self.db_test_priv.dbsession.commit()


if __name__ == "__main__":
    unittest.main()
