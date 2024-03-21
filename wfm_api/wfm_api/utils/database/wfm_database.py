"""This module defines the class WFMDatabase, that enables manipulation of
data for the WFM API.
"""
import time
from enum import Enum
from typing import Any, Dict, List, Tuple
from loguru import logger

from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.sql import text
from sqlalchemy.orm import relationship, backref
from sqlalchemy.orm import declarative_base

from wfm_api.utils.database.sqlite_database import SQLiteDatabase
from wfm_api.utils.errors import UnexistingSessionNameError, UnexistingServiceNameError
from wfm_api.utils.errors import NoDocumentError

__copyright__ = """
Copyright (C) Bull S.A.S.
"""

# Create the Baseclass of each of the database models class
# for the parent class of each of the database models (the ORM models).
Base = declarative_base()


class SessionStatus(str, Enum):
    """Enumeration class for the sessions status
    """
    STARTING = 'STARTING'
    ACTIVE = 'ACTIVE'
    STOPPING = 'STOPPING'
    STOPPED = 'STOPPED'
    TEARDOWN =  'TEARDOWN'
    UNKNOWN = 'UNKNOWN'


class Session(Base):
    """Class for Session table of WFM database.
    """
    __tablename__ = "session"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    user_name = Column(String)
    workflow_name = Column(String)
    start_time = Column(Integer) # start session timestamp
    end_time = Column(Integer) # end session timestamp
    status = Column(String) # starting/active/stopping/stopped
    services = relationship("Service", backref=backref("session"))
    step_descriptions = relationship("StepDescription", backref=backref("session"))

    def dict(self):
        """Get meaningful information for the end user.

        Args:

        Returns:
            Dict[str, Any]: Subset of the Session attributes.
        """
        return {
            "id": self.id,
            "workflow_name": self.workflow_name,
            "name": self.name,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "status": self.status
        }


class ServiceStatus(str, Enum):
    """Enumeration class for the services status
    """
    WAITING = 'WAITING'
    STAGINGIN = 'STAGINGIN'
    STAGEDIN = 'STAGEDIN'
    ALLOCATED = 'ALLOCATED'
    STAGINGOUT = 'STAGINGOUT'
    STAGEDOUT = 'STAGEDOUT'
    STOPPING = 'STOPPING'
    STOPPED = 'STOPPED'
    TEARDOWN =  'TEARDOWN'
    UNKNOWN = 'UNKNOWN'


class Service(Base):
    """Class for Service table of WFM database.
    """
    __tablename__ = "service"
    id = Column(Integer, primary_key=True)
    session_id = Column(Integer, ForeignKey("session.id"))
    step_descriptions = relationship("StepDescription", backref=backref("service"))
    name = Column(String)
    service_type = Column(String) # Enumerate
    location = Column(String)
    targets = Column(String)       # SBB only
    flavor = Column(String)        # SBB only
    namespace = Column(String)     # GBF & DASI only
    mountpoint = Column(String)    # GBF & DASI only
    storagesize = Column(String)   # GBF & DASI only
    datanodes = Column(Integer)
    start_time = Column(Integer) # start session timestamp
    end_time = Column(Integer) # end session timestamp
    status = Column(String) # staging in / staged in / allocated / staging_out / staged_out
    jobid = Column(Integer) # ID of the job that created the service

    def dict(self):
        """Get meaningful information for the end user.

        Args:

        Returns:
            Dict[str, Any]: Subset of the Service attributes.
        """

        srv_attributes = {
            "id": self.id,
            "session_id": self.session_id,
            "name": self.name,
            "type": self.service_type,
            "location": self.location,
            "status": self.status,
            "jobid": self.jobid
        }

        if self.service_type == 'NFS' or self.service_type == 'DASI':
            srv_attributes['namespace'] = self.namespace
            srv_attributes['mountpoint'] = self.mountpoint
        else:
            # SBB - default
            srv_attributes['targets'] = self.targets
    
        return srv_attributes


class NamespaceLock(Base):
    """Class for NamespaceLock table of WFM database.
    """
    __tablename__ = "namespace"
    id = Column(Integer, primary_key=True)
    ns_name = Column(String)
    service_name = Column(String)

    def dict(self):
        """Get meaningful information for the end user.

        Args:

        Returns:
            Dict[str, Any]: Subset of the Service attributes.
        """
        return {
            "id": self.id,
            "ns_name": self.ns_name,
            "service_name": self.service_name
        }


class StepDescription(Base):
    """Class for StepDescription table of WFM database.
    """
    __tablename__ = "step_description"
    id = Column(Integer, primary_key=True)
    session_id = Column(Integer, ForeignKey("session.id"))
    name = Column(String)
    command = Column(String)
    service_id = Column(Integer, ForeignKey("service.id"))
    steps = relationship("Step", backref=backref("step_description"))

    def dict(self):
        """Get meaningful information for the end user.

        Args:

        Returns:
            Dict[str, Any]: Subset of the StepDescription attributes.
        """
        return {
            "id": self.id,
            "session_id": self.session_id,
            "service_id": self.service_id,
            "name": self.name,
            "command": self.command
        }


class StepStatus(str, Enum):
    """Enumeration class for the steps status
    """
    STARTING = 'STARTING'
    RUNNING = 'RUNNING'
    STOPPING = 'STOPPING'
    STOPPED = 'STOPPED'
    SUSPENDED = 'SUSPENDED'


class Step(Base):
    """Class for Step table of WFM database.
    """
    __tablename__ = "step"
    id = Column(Integer, primary_key=True)
    instance_name = Column(String) # Unique step instance name <step name>_<index>
    step_description_id = Column(Integer, ForeignKey("step_description.id"))
    start_time = Column(Integer) # Step exec. starting timestamp
    stop_time = Column(Integer) # Step exec. end timestamp
    # TODO: declare it as an Enum when the set of allowed status is fixed
    # for the step_metadata model
    status = Column(String) # Step status as seen by the RM
    progress = Column(String) # Step progress in percent
    jobid = Column(Integer) # JobID of the job running the step
    command = Column(String) # Instanciated with any step level variable

    def dict(self):
        """Get meaningful information for the end user.

        Args:

        Returns:
            Dict[str, Any]: Subset of the Step attributes.
        """
        return {
            "id": self.id,
            "instance_name": self.instance_name,
            "status": self.status,
            "progress": self.progress,
            "jobid": self.jobid,
            "step_description_id": self.step_description_id
        }


class ObjectActivityLogging(Base):
    """Class for ObjectActivityLogging table of WFM database.
    """
    __tablename__ = "object_activity_logging"
    id = Column(Integer, primary_key=True)
    object_type = Column(String) # Object type: session /service / step_description / step
    object_id = Column(Integer) # Object Id: session id / service id / step_description id / step id
    activity = Column(String) # Activity on the object: creation / removal
    time = Column(Integer) # Activity timestamp

    def dict(self):
        """Get meaningful information for the end user.

        Args:

        Returns:
            Dict[str, Any]: Subset of the ObjectActivityLogging attributes.
        """
        return {
            "id": self.id,
            "object_type": self.object_type,
            "object_id": self.object_id,
            "activity": self.activity,
            "time": self.time
        }


class WFMDatabase(SQLiteDatabase):
    """Class for manipulation of data stored in the WFM database.
    """

    def __init__(self, name: str,  *args, **kwargs) -> None:
        """Creates wfm database with the Base model.

        Args:
            name (str): the sqlite database file.
        """
        super().__init__(name)

        # apply model
        db_model = Base.metadata.create_all(bind=self.engine)
        self.dbmodel = db_model

    def add_session(self,
                    ses_name: str,
                    wkf_name: str,
                    user_name: str,
                    ses_start: int,
                    ses_status: str) -> int:
        """Adds a session item into the Session table

        Args:
            ses_name (str): Session name
            wkf_name (str): Name of the Workflow the session belongs to
            user_name (str): Name of the user the session belongs to
            ses_start (int): Session starting time
            ses_status (str): Session status

        Returns:
            int: session id
        """
        item = Session(name=ses_name, workflow_name=wkf_name, user_name=user_name,
                       start_time=ses_start, status=ses_status)
        self.add_query(item)
        # Log this addition activity into the DB
        self.log_session_creation(item.id)
        return item.id

    def add_unique_session(self,
                           ses_name: str,
                           wkf_name: str,
                           user_name: str,
                           ses_start: int,
                           ses_status: str) -> int:
        """Adds a session item into the Session table if not already there

        Args:
            ses_name (str): Session name
            wkf_name (str): Workflow name
            user_name (str): User name
            ses_start (int): Session starting time
            ses_status (str): Session status

        Returns:
            int: session id
        """
        list_item = self.get_dicts_query(Session,
                                         f"user_name == '{user_name}' AND " +
                                         f"name == '{ses_name}' AND workflow_name == '{wkf_name}'")
        if list_item:
            logger.warning(f"session {ses_name} not added because already existing")
            return list_item[0]['id']

        return self.add_session(ses_name=ses_name,
                                wkf_name=wkf_name,
                                user_name=user_name,
                                ses_start=ses_start,
                                ses_status=ses_status)

    def get_all_sessions(self, uname: str = "") -> List[Dict[str, Any]]:
        """Returns all sessions.
        If a user name is provided, only sessions that belong to this user
        are returned.
        Args:
            uname (str, optional): User name. Defaults to ""
        Returns:
            List[Dict[str, Any]]: List of sessions.
        """
        if uname:
            result = self.get_dicts_query(Session, f"user_name == '{uname}'")
        else:
            result = self.get_dicts_query(Session)
        return result

    # TODO: the uname parameter should be mandatory once the authentication is implemented
    def get_session_info_from_name(self,
                                   sname: str,
                                   wname: str = "",
                                   uname: str = "") -> List[Dict[str, Any]]:
        """Given a session name, returns all sessions with that name.
        If a user name is provided, only sessions that belong to this user
        are returned.
        If a workflow name is provided, only sessions that belong to this workflow
        are returned.

        Args:
            sname (str): Session name
            wname (str, optional): Workflow name. Defaults to ""
            uname (str, optional): User name. Defaults to ""

        Returns:
            List[Dict[str, Any]]: List of sessions.
        """
        query = f"name == '{sname}'"
        if wname:
            query += f" AND workflow_name == '{wname}'"
        if uname:
            query += f" AND user_name == '{uname}'"
        result = self.get_dicts_query(Session, query)
        if len(result) > 0:
            return result

        if wname:
            raise UnexistingSessionNameError(sessionname=sname,
                                             workflowname=wname)

        raise UnexistingSessionNameError(sessionname=sname)

    # TODO: the uname parameter should be mandatory once the authentication is implemented
    def get_session_info_from_id(self,
                                 session_id: int,
                                 uname: str = "") -> List[Dict[str, Any]]:
        """Given a session id, returns all sessions with that id (should be unique).
        If a user name is provided, only the session with that id and that belongs to this user
        is returned.

        Args:
            session_id (str): Session id
            uname (str, optional): User name. Defaults to ""

        Returns:
            List[Dict[str, Any]]: List of sessions (empty list or singleton)
        """
        query = f"id == '{session_id}'"
        if uname:
            query += f" AND user_name == '{uname}'"
        return self.get_dicts_query(Session, query)

    def delete_session(self,
                       session_name: str = "",
                       session_id: int = 0) -> None:
        """Given a session name or a session id, deletes all sessions with this name or the
        session with this id from the Session table.

        Args:
            session_name (str, optional): Session name
            session_id (int, optional): Session id

        Returns:
            None
        """
        if session_name:
            query = f"name == '{session_name}'"
        else:
            query = f"id == '{session_id}'"
        sessions = self.get_objs_query(Session, query)
        for sess in sessions:
            # Log this removal activity into the DB
            self.log_session_removal(sess.dict()['id'])
            self.delete_query(sess)

    # TODO: to update session_status should use a unique field
    # name is not enough need user id also
    def update_session_status(self,
                              ses_name: str,
                              ses_status: str) -> None:
        """Given a session name, updates the corresponding item's status
        into the Session table.

        Args:
            ses_name (str): Session name
            ses_status (str): Session status

        Returns:
            None
        """
        self.update_status(Session, f"name == '{ses_name}'", ses_status)

    def add_nslock(self, namespace: str, srv_name: str) -> int:
        """Adds a namespace item into the NamespaceLock table

        Args:
            namespace (str): Namespace name
            srv_name (str): Service name

        Returns:
            int: namespace id
        """
        item = NamespaceLock(ns_name=namespace, service_name=srv_name)
        self.add_query(item)
        return item.id

    def delete_nslock(self, namespace: str) -> None:
        """Given a namespace, deletes the namespace lock with this name from the NamespaceLock table.

        Args:
            namespace (str): NamespaceLock to remove from the DB

        Returns:
            None
        """
        # Raises an exception if there are several items
        namespace = self.get_single_obj_query(NamespaceLock, f"ns_name == '{namespace}'")
        if not namespace:
            raise NoDocumentError(message=f"Error: Namespace {namespace} not found")
        self.delete_query(namespace)

    def get_ns_info_from_name(self, namespace: str) -> List[Dict[str, Any]]:
        """Given a namespace, returns all namespaces with that name.

        Args:
            namespace (str): Namespace name

        Returns:
            List[Dict[str, Any]]: List of namespaces.
        """
        return self.get_dicts_query(NamespaceLock, f"ns_name == '{namespace}'")

    def get_services_from_ns(self, namespace: str) -> List[str]:
        """Given a namespace, returns all service names that use that namespace.

        Args:
            namespace (str): Namespace name

        Returns:
            List[str]: List of service names.
        """
        namespaces = self.get_dicts_query(NamespaceLock, f"ns_name == '{namespace}'")
        if not namespaces:
            return[]
        return [ namespace['service_name'] for namespace in namespaces ]

    def add_service(self,
                    srv: Dict[str, Any],
                    srv_start: int,
                    srv_status: str,
                    srv_jobid: int) -> int:
        """Adds a service item into the Service table

        Args:
            srv (Dict[str, Any]): Service to store
            srv_start (int): Service start
            srv_status (str): Service status
            srv_jobid (str): Service starter job jobid

        Returns:
            int: service id
        """
        if 'datanodes' in srv['attributes'].keys():
            srv_datanodes = srv['attributes']['datanodes']
        else:
            srv_datanodes = 1
        if 'location' in srv['attributes'].keys():
            srv_location = srv['attributes']['location']
        else:
            srv_location = ''
        if srv['type'].upper() == 'NFS' or srv['type'].upper() == 'DASI':
            item = Service(name=srv['name'],
                           service_type=srv['type'].upper(),
                           location=srv_location,
                           targets='',
                           flavor='',
                           namespace=srv['attributes']['namespace'],
                           mountpoint=srv['attributes']['mountpoint'],
                           storagesize=srv['attributes']['storagesize'],
                           datanodes=srv_datanodes,
                           start_time=srv_start,
                           status=srv_status,
                           jobid=srv_jobid)
        else:
            # SBB - default
            item = Service(name=srv['name'],
                           service_type=srv['type'].upper(),
                           location=srv_location,
                           targets=srv['attributes']['targets'],
                           flavor=srv['attributes']['flavor'],
                           namespace='',
                           mountpoint='',
                           storagesize='',
                           datanodes=srv_datanodes,
                           start_time=srv_start,
                           status=srv_status,
                           jobid=srv_jobid)
        self.add_query(item)
        # Log this addition activity into the DB
        self.log_service_creation(item.id)
        return item.id

    def add_unique_service(self,
                           srv: Dict[str, Any],
                           srv_start: int,
                           srv_status: str,
                           srv_jobid: int) -> int:
        """Adds a service item into the Service table if not already there

        Args:
            srv (Dict[str, Any]): Service to store
            srv_start (int): Service start
            srv_status (str): Service status
            srv_jobid (str): Service starter jobid

        Returns:
            int: Service id
        """
        srv_name = srv['name']
        list_item = self.get_dicts_query(Service, f"name == '{srv_name}'")
        if list_item:
            item = list_item[0]
            return item['id']

        return self.add_service(srv=srv, srv_start=srv_start,
                                srv_status=srv_status, srv_jobid=srv_jobid)

    def get_all_services(self) -> List[Dict[str, Any]]:
        """Returns all services.

        Args:

        Returns:
            List[Dict[str, Any]]: List of services.
        """
        return self.get_dicts_query(Service)

    def get_service_info_from_name(self, name: str) -> List[Dict[str, Any]]:
        """Given a service name, returns all services with that name.

        Args:
            service_name (str): Service name

        Returns:
            List[Dict[str, Any]]: List of services.
        """
        result = self.get_dicts_query(Service, f"name == '{name}'")
        if len(result) > 0:
            return result

        raise UnexistingServiceNameError(servicename=name)

    def get_services_info_from_session_id(self, session_id: int) -> List[Dict[str, Any]]:
        """Given a session id, returns that session's services info.

        Args:
            session_id (int): Session id

        Returns:
            List[Dict[str, Any]]: List of Services.
                                  Empty list if no service meets the condition.
        """
        return self.get_dicts_query(Service, f"session_id == '{session_id}'")

    def get_service_info_from_id(self, service_id: int) -> List[Dict[str, Any]]:
        """Given a service id, returns the associated service info.

        Args:
            service_id (int): Service id

        Returns:
            List[Dict[str, Any]]: List of Service Items.
                                  Empty list if no service meets the condition.
        """
        return self.get_dicts_query(Service, f"id == '{service_id}'")

    def delete_service(self, srv_name: str) -> None:
        """Given a service name, deletes all services with this name from the Service table

        Args:
            srv_name (str): Service name

        Returns:
            None
        """
        services = self.get_objs_query(Service, f"name == '{srv_name}'")
        for srv in services:
            # Log this removal activity into the DB
            self.log_service_removal(srv.dict()['id'])
            self.delete_query(srv)

    def update_service_sessionid(self,
                                 srv_name: str,
                                 srv_sessid: int) -> None:
        """Given a service name, updates the corresponding item's session id
        into the Service table.

        Args:
            srv_name (str): Service name
            srv_sessid (str): Service session id

        Returns:
            None
        """
        self.update_sessionid(Service, f"name == '{srv_name}'", srv_sessid)

    def update_service_status(self,
                              srv_name: str,
                              srv_status: str) -> None:
        """Given a service name, updates the corresponding item's status
        into the Service table.

        Args:
            srv_name (str): Service name
            srv_status (str): Service status

        Returns:
            None
        """
        self.update_status(Service, f"name == '{srv_name}'", srv_status)

    def add_step_description(self,
                             step_sessid: int,
                             step_name: str,
                             step_command: str,
                             step_serviceid: int) -> int:
        """Adds a step description item into the StepDescription table

        Args:
            step_sessid (int): Session id this step belongs to
            step_name (str): Step name
            step_command (str): Step command
            step_serviceid (int): Service id this step uses

        Returns:
            int: StepDescription id
        """
        item = StepDescription(session_id=step_sessid,
                               name=step_name,
                               command=step_command,
                               service_id=step_serviceid,
                               steps=[])
        self.add_query(item)
        # Log this addition activity into the DB
        self.log_step_description_creation(item.id)
        return item.id

    def add_unique_step_description(self,
                                    step_sessid: int,
                                    step_name: str,
                                    step_command: str,
                                    service_name: str) -> int:
        """Adds a step description item into the StepDescription table if not already there

        Args:
            step_sessid (int): Session id this step belongs to
            step_name (str): Step name
            step_command (str): Step command
            service_name (str): Service used by the step

        Returns:
            int: StepDescription id
        """
        # Select on the step name AND the session id it belongs to
        list_item = self.get_dicts_query(StepDescription,
                                         f"name == '{step_name}' AND session_id == {step_sessid}")
        if list_item:
            item = list_item[0]
            return item['id']

        # Get the service id
        if len(service_name) > 0:
            service = self.get_service_info_from_name(service_name)
            srvid = service[0]['id']
        else:
            # Since SQL uses id's starting from 1, id 0 will have a special meaning for us:
            # this step doesn't use any service
            srvid = 0
        return self.add_step_description(step_sessid=step_sessid,
                                         step_name=step_name,
                                         step_command=step_command,
                                         step_serviceid=srvid)

    def get_all_steps_descriptions(self) -> List[Dict[str, Any]]:
        """Returns all steps descriptions.

        Args:

        Returns:
            List[Dict[str, Any]]: List of steps descriptions.
        """
        return self.get_dicts_query(StepDescription)

    def get_step_description_info_from_name(self, name: str) -> List[Dict[str, Any]]:
        """Given a step name, returns all steps descriptions with that name.

        Args:
            name (str): Step name

        Returns:
            List[Dict[str, Any]]: List of steps descriptions.
                                  Empty list if step name not found
        """
        return self.get_dicts_query(StepDescription, f"name == '{name}'")

    def get_step_descriptions_from_session_id(self, session_id: int) -> List[Dict[str, Any]]:
        """Given a session id, returns all steps descriptions with that session id.

        Args:
            session_id (int): Session id

        Returns:
            List[Dict[str, Any]]: List of steps descriptions.
                                  Empty list is if no step description meets the condition.
        """
        return self.get_dicts_query(StepDescription, f"session_id == '{session_id}'")

    def get_step_description(self, session_id: int, name: str) -> List[Dict[str, Any]]:
        """Given a session id and a step name, returns the corresponding steps
        descriptions.

        Args:
            session_id (int): Session id
            name (int): Step name

        Returns:
            List[Dict[str, Any]]: List of steps descriptions (should be a singleton in our context).
                                  Empty list is if no step description meets the condition.
        """
        return self.get_dicts_query(StepDescription,
                                    f"session_id == {session_id} and name == '{name}'")

    def get_step_description_from_id(self, stepd_id: int) -> List[Dict[str, Any]]:
        """Given a step description id, returns the corresponding steps
        descriptions.

        Args:
            stepd_id (int): StepDescription id

        Returns:
            List[Dict[str, Any]]: List of steps descriptions (should be a singleton).
                                  Empty list is if no step description meets the condition.
        """
        return self.get_dicts_query(StepDescription, f"id == {stepd_id}")

    def update_step_description_with_step(self, stepd_id: int, step_id: int) -> int:
        """Given a step description id and a step id, append the corresponding step object to the
        step description object's list of steps

        Args:
            stepd_id (int): StepDescription id to be updated
            step_id (int): Step id to add the list of steps

        Returns:
            int: 0 if successful
                 1 if query1 could not be fulfilled
                 2 if query2 could not be fulfilled
        """
        # Get the step descriptions to update (singleton since query by id)
        stepd_list = self.get_objs_query(StepDescription, f"id == '{stepd_id}'")
        if not stepd_list:
            return 1
        # Get the Steps to add to the step description (singleton since query by id)
        step_list = self.get_objs_query(Step, f"id == '{step_id}'")
        if not step_list:
            return 2
        stepd_list[0].steps.append(step_list[0])
        self.dbsession.commit() # commit the changes to the DB
        return 0

    def delete_step_description(self, step_descr_id: int) -> None:
        """Given a step description id, deletes all steps descriptions with this id
        from the StepDescription table.

        Args:
            step_descr_id (int): StepDescription id

        Returns:
            None
        """
        step_descriptions = self.get_objs_query(StepDescription, f"id == '{step_descr_id}'")
        # The id is unique, but loop however to cover the case where the list is empty
        for stp in step_descriptions:
            # Log this removal activity into the DB
            self.log_step_description_removal(stp.dict()['id'])
            self.delete_query(stp)

    def unique_step_instance_name(self, step_descrid: int) -> str:
        """ Build unique name for new step instance
        <user_name>-<session name>-<step name>_<index>

        Args:
            step_descrid (int): Step description id this step is related to

        Returns:
            str: The step instance name
            Raises exception upon error
        """
        index = 1
        step_descr = self.get_single_obj_query(StepDescription, f"id == {step_descrid}")
        if not step_descr:
            raise NoDocumentError(message=f"Error: Step description id {step_descrid} not found")
        session = self.get_single_obj_query(Session, f"id == {step_descr.session_id}")
        if not session:
            raise NoDocumentError(message=f"Error: Session id {step_descr.session_id} not found")

        steps = self.get_dicts_query(Step, f"step_description_id == {step_descrid}")
        if steps:
            index += len(steps)
        return  f'{session.user_name}-{session.name}-{step_descr.name}_{index}'

    def add_step(self,
                 step_descrid: int,
                 step_status: str,
                 step_progress: str,
                 step_command: str) -> Tuple[int, str]:
        """Adds a step item into the Step table

        Args:
            step_descrid (int): Step description id this step is related to
            step_status (str): Step status
            step_progress (str): Step progress
            step_command (str): Step command

        Returns:
            Tuple[int, str]: The step id and the step instance name
        """
        unique_step_instance_name = self.unique_step_instance_name(step_descrid)
        item = Step(step_description_id=step_descrid,
                    instance_name = unique_step_instance_name,
                    start_time=time.time_ns(),
                    status=step_status,
                    progress=step_progress,
                    command=step_command)
        self.add_query(item)
        # Log this addition activity into the DB
        self.log_step_creation(item.id)
        return item.id, unique_step_instance_name

    def get_all_steps(self) -> List[Dict[str, Any]]:
        """Returns all steps.

        Args:
           None

        Returns:
            List[Dict[str, Any]]: List of steps.
        """
        return self.get_dicts_query(Step)

    def delete_step(self, step_id: int) -> None:
        """Given a step id, delete it from the Step table.

        Args:
            step_id (int): Step id

        Returns:
            None
        """
        steps = self.get_objs_query(Step, f"id == '{step_id}'")
        # The id is unique, but loop however to cover the case where the list is empty
        for stp in steps:
            # Log this removal activity into the DB
            self.log_step_removal(stp.dict()['id'])
            self.delete_query(stp)

    def update_step_status(self,
                           step_id: int,
                           step_status: str) -> None:
        """Given a step id, updates the corresponding item's status into the Step table.

        Args:
            step_id (int): Step id
            step_status (str): New step status

        Returns:
            None
        """
        self.update_status(Step, f"id == {step_id}", step_status)

    def update_step_jobid(self,
                          step_id: int,
                          jobid: int) -> None:
        """Given a step id, updates the corresponding item's jobid into the Step table.

        Args:
            step_id (int): Step id
            jobid (str): New step jobid

        Returns:
            None
        """
        self.dbsession.query(Step).filter(text(f"id == {step_id}")).update({"jobid": jobid},
                                                                           synchronize_session="fetch")
        self.dbsession.commit() # commit the changes to the DB

    def update_step_progress(self,
                             step_id: int,
                             progress: str) -> None:
        """Given a step id, updates the corresponding item's progress into the Step table.

        Args:
            step_id (int): Step id
            progress (str): New step progress

        Returns:
            None
        """
        self.dbsession.query(Step).filter(text(f"id == {step_id}")).update({"progress": progress},
                                                                           synchronize_session="fetch")
        self.dbsession.commit() # commit the changes to the DB

    def get_steps_info_from_session_id(self, session_id: int) -> List[Dict[str, Any]]:
        """Given a session id, returns that session's steps instances info.

        Args:
            session_id (int): Session id

        Returns:
            list[Dict[str, Any]]: List of Steps instances.
                                  Empty list if no step meets the condition
        """
        # First look for the StepDescriptions from the session id.
        # Then look for the steps given each step description id.
        stepds = self.get_dicts_query(StepDescription, f"session_id == '{session_id}'")
        all_steps = []
        for stepd in stepds:
            steps = self.get_dicts_query(Step, f"step_description_id == '{stepd['id']}'")
            all_steps += steps
        return all_steps

    def get_steps_info_from_step_description_id(self,
                                                step_description_id: int) -> List[Dict[str, Any]]:
        """Given a step description id, returns the associated steps instances info.

        Args:
            step_description_id (int): StepDescription id

        Returns:
            list[Dict[str, Any]]: List of Steps instances.
                                  Empty list if no step meets the condition
        """
        return self.get_dicts_query(Step, f"step_description_id == '{step_description_id}'")

    def get_steps_info_from_step_description_name(self, session_id: int,
                                                  stepd_name: str) -> List[Dict[str, Any]]:
        """Given a session id and a step name, returns the corresponding step instances.

        Args:
            session_id (int): Session id
            stepd_name (str): Step description name

        Returns:
            List[Dict[str, Any]]: List of step instances.
                                  Empty list if there is no step instance for the step.
        """
        step_descr = self.get_single_obj_query(StepDescription, f"session_id == '{session_id}' AND "
                                               + f"name == '{stepd_name}'")
        if not step_descr:
            raise NoDocumentError(message=f"Error: No {stepd_name} step for {session_id} session")
        return self.get_dicts_query(Step, f"step_description_id == {step_descr.id}")

    def get_steps_info_from_jobid(self, jobid: int) -> List[Dict[str, Any]]:
        """Given a jobid, returns the associated steps instances info.

        Args:
            jobid (int): job id

        Returns:
            List[Dict[str, Any]]: List of Steps instances.
                                  Empty list if no step meets the condition
        """
        return self.get_dicts_query(Step, f"jobid == '{jobid}'")

    def log_session_creation(self,
                             session_id: int) -> int:
        """Logs the creation of a session object

        Args:
            session_id (int): Session id

        Returns:
            int: Object Activity Logging id
        """
        item = ObjectActivityLogging(object_type="session",
                                     object_id=session_id,
                                     activity="creation",
                                     time=time.time_ns())
        self.add_query(item)
        return item.id

    def log_session_removal(self,
                            session_id: int) -> None:
        """Logs the removal of a session object

        Args:
            session_id (int): Session id

        Returns:
            int: Object Activity Logging id
        """
        item = ObjectActivityLogging(object_type="session",
                                     object_id=session_id,
                                     activity="removal",
                                     time=time.time_ns())
        self.add_query(item)
        return item.id

    def log_service_creation(self,
                             service_id: int) -> int:
        """Logs the creation of a service object

        Args:
            service_id (int): Service id

        Returns:
            int: Object Activity Logging id
        """
        item = ObjectActivityLogging(object_type="service",
                                     object_id=service_id,
                                     activity="creation",
                                     time=time.time_ns())
        self.add_query(item)
        return item.id

    def log_service_removal(self,
                            service_id: int) -> None:
        """Logs the removal of a service object

        Args:
            service_id (int): Service id

        Returns:
            int: Object Activity Logging id
        """
        item = ObjectActivityLogging(object_type="service",
                                     object_id=service_id,
                                     activity="removal",
                                     time=time.time_ns())
        self.add_query(item)
        return item.id

    def log_step_description_creation(self,
                                      step_description_id: int) -> int:
        """Logs the creation of a step_description object

        Args:
            step_description_id (int): StepDescription id

        Returns:
            int: Object Activity Logging id
        """
        item = ObjectActivityLogging(object_type="step_description",
                                     object_id=step_description_id,
                                     activity="creation",
                                     time=time.time_ns())
        self.add_query(item)
        return item.id

    def log_step_description_removal(self,
                                     step_description_id: int) -> None:
        """Logs the removal of a step_description object

        Args:
            step_description_id (int): StepDescription id

        Returns:
            int: Object Activity Logging id
        """
        item = ObjectActivityLogging(object_type="step_description",
                                     object_id=step_description_id,
                                     activity="removal",
                                     time=time.time_ns())
        self.add_query(item)
        return item.id

    def log_step_creation(self, step_id: int) -> int:
        """Logs the creation of a step object

        Args:
            step_id (int): Step id

        Returns:
            int: Object Activity Logging id
        """
        item = ObjectActivityLogging(object_type="step",
                                     object_id=step_id,
                                     activity="creation",
                                     time=time.time_ns())
        self.add_query(item)
        return item.id

    def log_step_removal(self,
                         step_id: int) -> None:
        """Logs the removal of a step object

        Args:
            step_id (int): StepDescription id

        Returns:
            int: Object Activity Logging id
        """
        item = ObjectActivityLogging(object_type="step",
                                     object_id=step_id,
                                     activity="removal",
                                     time=time.time_ns())
        self.add_query(item)
        return item.id
