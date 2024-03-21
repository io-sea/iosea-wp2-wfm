"""This module contains a wrapper around the SQLiteDatabase, for manipulating data
from the sql database.
"""
__copyright__ = """
Copyright (C) 2022 Bull S. A. S. - All rights reserved
Bull, Rue Jean Jaures, B.P.68, 78340, Les Clayes-sous-Bois, France
This is not Free or Open Source software.
Please contact Bull S. A. S. for details about its license.
"""

from typing import Any, Dict, List
from loguru import logger

from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text

from pax.utils.database.database import Database
from wfm_api.utils.errors import NoUniqueDocumentError

EMPTY_DOC_ERROR_MSG = "Received empty document from database."


class SQLiteDatabase(Database):
    """Class for the manipulation of data stored in a sqlite Database through SQLAlchemy connection.
    It inherits from the Database class for attributes initialization.
    """

    def __init__(self, name: str) -> None:
        """Creates an engine and a session.

        Args:
            name (str): the sqlite database file.
        """
        super().__init__(name)

        try:
            # In FastAPI, more than one thread could interact with the
            # database for the same request, so for SQLite needs to
            # allow that with connect_args={"check_same_thread": False}.
            self.engine = create_engine(f"sqlite:///{name}",
                connect_args={"check_same_thread": False})
            self.engine.connect()
        except SQLAlchemyError as exc:
            logger.critical(f"Unable to open the {name} database.")
            raise PermissionError from exc

        logger.info(f"Succesfully connected to Sqlite database {name}")

        # create session
        db_session = sessionmaker(bind=self.engine)
        self.dbsession = db_session()

    def add_query(self,
                  item: Dict[str, Any]) -> None:
        """Add an entry to an SQL database (for POST API query)

        Args:
            item (Dict[str, Any]): The entry to add
        """
        self.dbsession.add(item)
        self.dbsession.commit() # commit the changes to the DB
        self.dbsession.refresh(item) # expire and refresh the item attributes

    def get_single_obj_query(self,
                             table_name: Any,
                             text_query_filter: str) -> Any:
        """Retrieve single entry from table_name matching the
        query filter

        Args:
            table_name (Any): The table (class name) to retrieve data from.
            text_query_filter (str): The sql condition.

        Returns:
            Any: The retrieved result.
        """
        result = self.dbsession.query(table_name).filter(text(text_query_filter)).all()
        if not result:
            logger.debug(EMPTY_DOC_ERROR_MSG)
            return None
        if len(result) == 1:
            return result[0]

        raise NoUniqueDocumentError()

    def get_objs_query(self,
                       table_name: Any,
                       text_query_filter: str = None) -> List[Any]:
        """Retrieve entries from a SQL database for get API query

        Args:
            table_name (Any): The table (class name) to retrieve data from.
            text_query_filter (str): The sql condition.
                Defaults to None, corresponding to get all entries.

        Returns:
            List[Any]: The retrieved result, as list of DB objects.
                This list can be empty, in which case a warning is raised.
        """
        if text_query_filter:
            result = self.dbsession.query(table_name).filter(text(text_query_filter)).all()
        else:
            result = self.dbsession.query(table_name).all()
        if not result:
            logger.debug(EMPTY_DOC_ERROR_MSG)
        return result

    def get_dicts_query(self,
                        table_sqlite: Any,
                        text_query_filter: str = None) -> List[Dict[str, Any]]:
        """Retrieve entries from a SQLite database for get API query

        Args:
            table_sqlite (Any): The table (class name) to retrieve data from.
            query (str): The sql condition.
                Defaults to None, corresponding to get all entries.

        Returns:
            List[Dict[str, Any]]: The retrieved result, which can be empty, in which
                case a debug message is raised.
        """
        return [item.dict() for item in self.get_objs_query(table_sqlite, text_query_filter)]

    def delete_query(self,
                     item: Dict[str, Any]) -> None:
        """Delete an entry from an SQL database

        Args:
            item (Dict[str, Any]): The entry to delete.

        Returns:
            None
        """
        self.dbsession.delete(item)
        self.dbsession.commit() # commit the changes to the DB

    def update_sessionid(self,
                         table_name: Any,
                         update_filter: str,
                         sessionid: int) -> None:
        """Update an entry into an SQL database

        Args:
            table_name (Any): The table (class name) to update data into.
            update_filter (str): The sql condition.
            sessionid (int): The new session id.

        Returns:
            None
        """
        self.dbsession.query(table_name).filter(text(update_filter)).update({"session_id": sessionid},
                                                                            synchronize_session="fetch")
        self.dbsession.commit() # commit the changes to the DB

    def update_status(self,
                      table_name: Any,
                      update_filter: str,
                      status: str) -> None:
        """Update an entry status into an SQL database

        Args:
            table_name (Any): The table (class name) to update data into.
            update_filter (str): The sql condition.
            status (str): The new status.

        Returns:
            None
        """
        self.dbsession.query(table_name).filter(text(update_filter)).update({"status": status},
                                                                            synchronize_session="fetch")
        self.dbsession.commit() # commit the changes to the DB
