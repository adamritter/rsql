import sys
sys.path.append("src")
import unittest
from rsql import Database, Row

class TestRow(unittest.TestCase):
    def setUp(self):
        # Create an in-memory database
        self.db = Database(':memory:')
        # Create a table
        self.table = self.db.table('test_table', id='INTEGER PRIMARY KEY', name='TEXT')
        # Insert a row into the table
        self.table.insert(id=1, name='test')

    def tearDown(self):
        # Close the database connection
        del self.db

    def test_row_init(self):
        # Retrieve the row from the table
        row = self.table.fetchone(id=1)
        self.assertIsNotNone(row)
        print("row", row)
        self.assertEqual(row['id'], 1)
        self.assertEqual(row['name'], 'test')

    def test_row_getattr(self):
        row = self.table.fetchone(id=1)
        self.assertEqual(row.id, 1)
        self.assertEqual(row.name, 'test')

    def test_row_setattr(self):
        row = self.table.fetchone(id=1)
        row.name = 'changed'
        self.assertEqual(row.name, 'changed')
        # Verify that the change is persisted in the database
        updated_row = self.table.fetchone(id=1)
        self.assertEqual(updated_row.name, 'changed')

    def test_row_delete(self):
        row = self.table.fetchone(id=1)
        row.delete()
        # Attempt to retrieve the deleted row
        deleted_row = self.table.fetchone(id=1)
        self.assertIsNone(deleted_row)

    def test_row_update(self):
        row = self.table.fetchone(id=1)
        row.update(name='updated')
        updated_row = self.table.fetchone(id=1)
        self.assertEqual(updated_row.name, 'updated')

    def test_row_iteration(self):
        row = self.table.fetchone(id=1)
        keys = [k for k in row]
        self.assertEqual(set(keys), {'id', 'name'})

    def test_row_items(self):
        row = self.table.fetchone(id=1)
        items = list(row.items())
        self.assertIn(('id', 1), items)
        self.assertIn(('name', 'test'), items)

    def test_row_dict(self):
        row = self.table.fetchone(id=1)
        row_dict = row.__dict__()
        self.assertEqual(row_dict, {'id': 1, 'name': 'test'})


if __name__ == '__main__':
    unittest.main(verbosity=2)
