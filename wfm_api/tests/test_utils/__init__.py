"""Create WFM database for tests
"""
import os
from wfm_api.utils.database.wfm_database import Session, Service, WFMDatabase
from wfm_api.utils.database.wfm_database import StepDescription, Step
from wfm_api.utils.database.wfm_database import ObjectActivityLogging

__copyright__ = """
Copyright (C) Bull S.A.S.
"""


TEST_DATABASE = f"/tmp/{os.environ['USER']}-wfm_test.db"

# remove test database if exists
if os.path.exists(TEST_DATABASE):
    os.remove(TEST_DATABASE)

# create test database
test_db = WFMDatabase(name=TEST_DATABASE)

session_s1 = Session(name="s1", workflow_name="test1", user_name='user',
                     start_time=0, end_time=0, status="starting")
session_s2 = Session(name="s2", workflow_name="test1", user_name='user',
                     start_time=0, end_time=0, status="starting")

service_e1 = Service(name="e1", session_id=session_s1, service_type="SBB", location="location1",
                     targets="/target1", flavor="flavor1",
                     datanodes=1, start_time=0, end_time=0, status="status1",
                     jobid=1)
service_e2 = Service(name="e2", session_id=session_s2, service_type="SBB", location="location2",
                     targets="/target2", flavor="flavor2",
                     datanodes=2, start_time=0, end_time=0, status="status2",
                     jobid=2)

stepd_a1 = StepDescription(name="a", session_id=session_s1, command="command1",
                           service_id=service_e1.id)
step_a10 = Step(step_description_id=stepd_a1, instance_name="s1_a_1",
                start_time=10, stop_time=10, status="status10", progress="Preparing")
step_a11 = Step(step_description_id=stepd_a1, instance_name="s1_a_2",
                start_time=11, stop_time=11, status="status11", jobid=1100, progress="Syncing")

stepd_a2 = StepDescription(name="a", session_id=session_s2, command="command2",
                           service_id=service_e2.id)
step_a20 = Step(step_description_id=stepd_a2, instance_name="s2_a_1",
                start_time=20, stop_time=20, status="status20", jobid=2000, progress="Copying 20%")

session_s1.services = [service_e1]
session_s2.services = [service_e2]

service_e1.step_descriptions = [stepd_a1]
service_e2.step_descriptions = [stepd_a2]

session_s1.step_descriptions = [stepd_a1]
session_s2.step_descriptions = [stepd_a2]

stepd_a1.steps = [step_a10, step_a11]
stepd_a2.steps = [step_a20]

# ObjectActivity for sessions s1 and s2, services e1 and e2 step_descriptions a1 and a2
oa_s1 = ObjectActivityLogging(object_type="session", object_id=session_s1.id, activity="creation",
                              time=123)
oa_s2 = ObjectActivityLogging(object_type="session", object_id=session_s2.id, activity="creation",
                              time=123)
oa_e1 = ObjectActivityLogging(object_type="service", object_id=service_e1.id, activity="creation",
                              time=123)
oa_e2 = ObjectActivityLogging(object_type="service", object_id=service_e2.id, activity="creation",
                              time=123)
oa_a1 = ObjectActivityLogging(object_type="step_description", object_id=stepd_a1.id,
                              activity="creation", time=123)
oa_a2 = ObjectActivityLogging(object_type="step_description", object_id=stepd_a2.id,
                              activity="creation", time=123)
oa_a10 = ObjectActivityLogging(object_type="step", object_id=step_a10.id, activity="creation",
                              time=123)
oa_a11 = ObjectActivityLogging(object_type="step", object_id=step_a11.id, activity="creation",
                              time=123)
oa_a20 = ObjectActivityLogging(object_type="step", object_id=step_a20.id, activity="creation",
                              time=123)

test_db.dbsession.add(session_s1)
test_db.dbsession.add(session_s2)

test_db.dbsession.add(service_e1)
test_db.dbsession.add(service_e2)

test_db.dbsession.add(stepd_a1)
test_db.dbsession.add(stepd_a2)

test_db.dbsession.add(step_a10)
test_db.dbsession.add(step_a11)
test_db.dbsession.add(step_a20)

test_db.dbsession.commit()
test_db.dbsession.close()
