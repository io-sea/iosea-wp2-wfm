"""Create WFM database for tests
"""
import os
import time

from wfm_api.utils.database.wfm_database import Session, Service, Base, WFMDatabase
from wfm_api.utils.database.wfm_database import StepDescription, Step
from wfm_api.utils.database.wfm_database import ObjectActivityLogging

# create database in /tmp
wfm_db = WFMDatabase(name=f"/tmp/wfm_db-{os.getuid()}")

Base.metadata.create_all(bind=wfm_db.engine)

session_s1 = Session(name="s1", workflow_name="test1", user_name='user',
                     start_time=0, end_time=0, status="starting")
session_s2 = Session(name="s2", workflow_name="test1", user_name='user',
                     start_time=0, end_time=0, status="starting")

service_e1 = Service(session_id=session_s1, name="e1", service_type="SBB", location='location1',
                     targets="/tmp", flavor="small",
                     datanodes=1, start_time=0, end_time=0, status="allocated",
                     jobid=1)
service_e2 = Service(session_id=session_s2, name="e2", service_type="SBB", location='location2',
                     targets="/target2", flavor="flavor2",
                     datanodes=2, start_time=0, end_time=0, status="status2",
                     jobid=2)

stepd_a1 = StepDescription(name="a1", session_id=session_s1, command="command1", service_id=service_e1)
step_a10 = Step(step_description_id=stepd_a1, instance_name="s1_a_1",
                start_time=10, stop_time=10, status="status10", "")
step_a11 = Step(step_description_id=stepd_a1, instance_name="s1_a_2",
                start_time=11, stop_time=11, status="status11", "")

stepd_a2 = StepDescription(name="a2", session_id=session_s2, command="command2", service_id=service_e2)
step_a20 = Step(step_description_id=stepd_a2, instance_name="s2_a_1",
                start_time=20, stop_time=20, status="status20", "")

session_s1.services = [service_e1]
session_s2.services = [service_e2]
session_s1.step_descriptions = [stepd_a1]
session_s2.step_descriptions = [stepd_a2]
service_e1.step_descriptions = [stepd_a1]
service_e2.step_descriptions = [stepd_a2]
stepd_a1.steps = [step_a10, step_a11]
stepd_a2.steps = [step_a20]

oa_s1 = ObjectActivityLogging(object_type="session", object_id=session_s1.id,
                              activity="creation", time=time.time_ns())
oa_s2 = ObjectActivityLogging(object_type="session", object_id=session_s2.id,
                              activity="creation", time=time.time_ns())
oa_e1 = ObjectActivityLogging(object_type="service", object_id=service_e1.id,
                              activity="creation", time=time.time_ns())
oa_e2 = ObjectActivityLogging(object_type="service", object_id=service_e2.id,
                              activity="creation", time=time.time_ns())
oa_a1 = ObjectActivityLogging(object_type="step_description", object_id=stepd_a1.id,
                              activity="creation", time=time.time_ns())
oa_a2 = ObjectActivityLogging(object_type="step_description", object_id=stepd_a2.id,
                              activity="creation", time=time.time_ns())
oa_a10 = ObjectActivityLogging(object_type="step", object_id=step_a10.id,
                               activity="creation", time=time.time_ns())
oa_a11 = ObjectActivityLogging(object_type="step", object_id=step_a11.id,
                               activity="creation", time=time.time_ns())
oa_a20 = ObjectActivityLogging(object_type="step", object_id=step_a20.id,
                               activity="creation", time=time.time_ns())

wfm_db.dbsession.add(session_s1)
wfm_db.dbsession.add(session_s2)

wfm_db.dbsession.add(service_e1)
wfm_db.dbsession.add(service_e2)

wfm_db.dbsession.add(stepd_a1)
wfm_db.dbsession.add(stepd_a2)

wfm_db.dbsession.add(step_a10)
wfm_db.dbsession.add(step_a11)
wfm_db.dbsession.add(step_a20)

wfm_db.dbsession.commit()
wfm_db.dbsession.close()

# use created database
wfm_db_test = WFMDatabase(name=f"/tmp/wfm_db-{os.getuid()}")
print("=============== ALL SESSIONS ================")
print(wfm_db_test.get_all_sessions())
print("")

print("=============== ALL SERVICES ================")
print(wfm_db_test.get_all_services())

print("=============== ALL STEP DESCRIPTIONS ================")
print(wfm_db_test.get_all_steps_descriptions())

print("=============== ALL STEPS ================")
print(wfm_db_test.get_all_steps())
wfm_db.dbsession.close()
