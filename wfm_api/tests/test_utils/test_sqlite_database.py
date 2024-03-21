"""Unit tests for the sqlite_database connection module.
"""

import unittest
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import declarative_base

from wfm_api.utils.database.sqlite_database import SQLiteDatabase
from wfm_api.utils.errors import NoUniqueDocumentError

# Create sample model for test SQLite database
Base = declarative_base()


class SampleTable(Base):
    """Class for the sample table of the test database.
    """
    __tablename__ = "sample"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    description = Column(String)

    def dict(self):
        """Get information from SampleTable.

        Args:

        Returns:
            Dict[str, Any]: SampleTable attributes.
        """
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description
        }


class TestSQLiteDatabase(unittest.TestCase):
    """Tests that the manipulation of the sqlite database behaves as expected.
    """

    def setUp(self):
        """Create database for tests.
        """
        self.db_test = SQLiteDatabase(name=":memory:")
        Base.metadata.create_all(bind=self.db_test.engine)
        item1 = SampleTable(name="item1", description="test item1")
        self.db_test.dbsession.add(item1)
        self.db_test.dbsession.commit()
        self.db_test.dbsession.refresh(item1)
        self.item = item1

    def test_init_error(self):
        """Tests raise critical message if unable to open database"""
        with self.assertRaises(PermissionError):
            SQLiteDatabase('/var/toto')

    def test_get_single_obj_query(self):
        """Tests that getting an object from a specific table behaves as expected"""

        # one item
        query_filter = "name == 'item1'"
        result = self.db_test.get_single_obj_query(SampleTable, query_filter)
        self.assertEqual(result, self.item)
        # two items
        new_item = SampleTable(name= 'item1', description='second item1')
        self.db_test.add_query(new_item)
        query_filter = "name == 'item1'"
        with self.assertRaises(NoUniqueDocumentError):
            self.db_test.get_single_obj_query(SampleTable, query_filter)
        # no item
        query_filter = "name == 'unexisting'"
        result = self.db_test.get_single_obj_query(SampleTable, query_filter)
        self.assertEqual(result, None)


    def test_get_dicts_query(self):
        """Tests that getting all the rows from a specific table behaves as expected"""

        expected_list = [
            {'id': 1, 'name': 'item1', 'description': 'test item1'}]
        result_all = self.db_test.get_dicts_query(SampleTable)
        query_filter = "name == 'item1'"
        result_filter1 = self.db_test.get_dicts_query(SampleTable, query_filter)
        query_filter2 = "name == 'unexisting'"
        result_filter2 = self.db_test.get_dicts_query(SampleTable, query_filter2)

        self.assertListEqual(result_all, expected_list)
        self.assertListEqual(result_filter1, expected_list)
        self.assertListEqual(result_filter2, [])

    def test_add_query(self):
        """Tests that adding an element to a specific table behaves as expected"""
        new_item = SampleTable(name= 'item2', description='test item2')
        self.db_test.add_query(new_item)
        expected_list = [
            {'id': 1, 'name': 'item1', 'description': 'test item1'},
            {'id': 2, 'name': 'item2', 'description': 'test item2'}]
        result_all = self.db_test.get_dicts_query(SampleTable)
        self.assertListEqual(result_all, expected_list)

    def test_delete_query(self):
        """Tests that deleting an element from a specific table behaves as expected"""
        new_item = SampleTable(name= 'item2', description='test item2')
        self.db_test.dbsession.add(new_item)
        self.db_test.dbsession.commit()
        self.db_test.dbsession.refresh(new_item)
        expected_list = [
            {'id': 1, 'name': 'item1', 'description': 'test item1'},
            {'id': 2, 'name': 'item2', 'description': 'test item2'}]
        result_all = self.db_test.get_dicts_query(SampleTable)
        self.assertListEqual(result_all, expected_list)
        self.db_test.delete_query(new_item)
        expected_list = [ {'id': 1, 'name': 'item1', 'description': 'test item1'} ]
        result_all = self.db_test.get_dicts_query(SampleTable)
        self.assertListEqual(result_all, expected_list)


if __name__ == "__main__":
    unittest.main()
