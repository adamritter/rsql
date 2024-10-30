"""
RSQL: Reactive SQL Wrapper

This module provides a reactive SQL wrapper for light web development.
It tracks and propagates changes from database tables to views.

Key features:
- Live SQL updates
- Reactive views
- Support for SQLite database operations
- Custom data types and constraints

Usage:
    import rsql

    # Create a database
    db = rsql.Database('example.db')

    # Define tables and views
    # ...

Classes:
    - Database
    - View
    - Table
    - SQLUnion
    - EditableRowItem
    - URLM
    ...

Functions:
    - execute
    - create_where_null_clause
    - sqlrepr
    - track_views
    ...

Constants:
    - DEBUG
    - DEBUG_VIEWS
    - AUTOINCREMENT
    - CHECK_SAME_THREAD
    ...
"""

# Live SQL wrapper
# The main goal of the live sql wrapper is to track and propagate changes from database tables to views.
# It's meant for light web development

# TODO:
# - Worse is better: go for simple implementation over simple interface / API (HTML over QT)

# - rsql github repo
# - implement weather app to prove out the framework instead of random apps (try QT and web version at the same time)
# - live QT programming?
# - implement focus store / restore for rows.
# - Fix weak methods: how to remove them efficiently? Probably check for weakref in cbs when calling them.
# - Also support onupdate for rows? It will help for QT binding. For web, not sure, but not unsure either.
#   I have thorught a lot about implementing it, there are multiple ways.
#   I now think that for ,,rowfn'' to work, every descendant of row should know about local updating of a row
#  (that's what I would call it). In a row_fn some fields can be local updated, for others
#  just global update can be done which needs rerendering the whole row.
# - database editor
# - query editor
# - schema editor
# - Python vs Ruby comparision (ruby 2x slower, probably a bit nicer API, but not that much difference. insert/delete much slower for some reason, also need prepare statement)
import os, threading, traceback
from urllib.parse import urlencode

DEBUG = os.environ.get('DEBUG', 'False').lower() in ['true', '1', 'yes', 'on']
DEBUG_VIEWS = os.environ.get('DEBUG_VIEWS', 'False').lower() in ['true', '1', 'yes', 'on']

import sqlite3, importlib, sys, time, typing, weakref
from typing import Optional, Union, Type
def reload(): importlib.reload(sys.modules[__name__])
def timed(f):
    start = time.time()
    f()
    return time.time() - start

def istartswith(s: str, prefix: str) -> bool:
    return s[0:len(prefix)].lower() == prefix.lower()

def weak_cb(cb):
    return weakref.WeakMethod(cb)


class URLM:
    """
    URLM class represents a URL with a method.

    Attributes:
        url (str): The URL.
        method (str): The HTTP method.
    """
    def __init__(self, url, method):
        """
        Initialize the URLM object.

        Args:
            url (str): The URL.
            method (str): The HTTP method.
        """
        self.url = url
        self.__method__ = method
    
    def __str__(self):
        """
        Return the string representation of the URLM object.

        Returns:
            str: The URL.
        """
        return self.url

def execute(cursor, query, params=None):
    """
    Execute a SQL query on the database.

    Args:
        cursor: The cursor object.
        query (str): The SQL query to execute.
        params (tuple, optional): Parameters for the query.

    Returns:
        cursor: The cursor object after executing the query.
    """
    if DEBUG:
        if params:
            print(query, params)
        else:
            print(query)
    try:
        if params:
            return cursor.execute(query, params)
        else:
            return cursor.execute(query)
    except Exception as e:
        raise Exception(f"Error executing query: {query} with params {params}") from e

    
def create_where_null_clause(columns, values):
    """
    Create a WHERE clause for NULL values.

    Args:
        columns (list): List of column names.
        values (list): List of corresponding values.

    Returns:
        tuple: A tuple containing the WHERE clause and the remaining values.
    """
    where_conditions = []
    remaining_values = []
    for col, val in zip(columns, values):
        if val is None:
            where_conditions.append(f'{col} IS NULL')
        else:
            where_conditions.append(f'{col} = ?')
            remaining_values.append(val)
    
    where_clause = ' AND '.join(where_conditions) if where_conditions else ''
    return f" WHERE {where_clause}" if where_clause else '', tuple(remaining_values)


def sqlrepr(obj):
    if obj == None:
        return "NULL"
    elif isinstance(obj, bool):
        return "1" if obj else "0"
    else:
        return obj.__repr__()

# Define a mapping from Python types to SQLite types
python_to_sqlite_type = {
    int: "INTEGER",
    float: "REAL",
    str: "TEXT",
    bytes: "BLOB",
    bool: "BOOLEAN",  # Store booleans as integers (0 and 1)
}

AUTOINCREMENT = "INTEGER PRIMARY KEY AUTOINCREMENT"
def primary_key(dtype: Union[str, Type]) -> str:
    return f"{dtype if isinstance(dtype, str) else python_to_sqlite_type[dtype]} PRIMARY KEY"
def not_null(dtype: Union[str, Type]) -> str:
    return f"{dtype if isinstance(dtype, str) else python_to_sqlite_type[dtype]} NOT NULL"
def unique(dtype: Union[str, Type]) -> str:
    return f"{dtype if isinstance(dtype, str) else python_to_sqlite_type[dtype]} UNIQUE"
def foreign_key(dtype: Union[str, Type], table: str, column: str) -> str:
    return f"{dtype if isinstance(dtype, str) else python_to_sqlite_type[dtype]} REFERENCES {table}({column})"
def check(dtype: Union[str, Type], condition: str) -> str:
    return f"{dtype if isinstance(dtype, str) else python_to_sqlite_type[dtype]} CHECK ({condition})"
def default(dtype: Union[str, Type], value) -> str:
    return f"{dtype if isinstance(dtype, str) else python_to_sqlite_type[dtype]} DEFAULT {value}"
def index(dtype: Union[str, Type]) -> str:
    return f"{dtype if isinstance(dtype, str) else python_to_sqlite_type[dtype]} INDEX"
def unique_index(dtype: Union[str, Type]) -> str:
    return f"{dtype if isinstance(dtype, str) else python_to_sqlite_type[dtype]} UNIQUE INDEX"
def collate(dtype: Union[str, Type], collation: str) -> str:
    return f"{dtype if isinstance(dtype, str) else python_to_sqlite_type[dtype]} COLLATE {collation}"

# EXPERIMENTAL_OUTER_JOIN = False
# from . import EXPERIMENTAL_OUTER_JOIN

import time

class EditableRowItem:
    def __init__(self, value, onchange=None, local_change=False):
        self.value = value
        self.onchange = onchange
        self.local_change_cb = [] if local_change else None

    def onvalue(self, cb):
        if self.local_change_cb is not None:
            self.local_change_cb.append(cb)
        cb(self.value)

    def set(self, value):
        if self.value != value:
            self.value = value
            if self.onchange:
                t = time.time()
                self.onchange(value)
                # print(f"onchange took {(time.time() - t) * 1000}ms")
    
    def __repr__(self):
        return f"<EditableRowItem {self.value}>"
    
    def __str__(self):
        return str(self.value)
    
    def local_updated(self, value):
        if self.value != value and self.local_change_cb is not None:
            self.value = value
            for cb in self.local_change_cb:
                cb(value)

IN_PROGRESS = False

# If LOCAL_CHANGE is True and a row is updated, the change is propogated to all EditableRowItems.
LOCAL_CHANGE = False # Buggy

class Row:
    def __init__(self, values, __table__=None):
        self.__table__ = __table__
        if isinstance(values, Row):
            values = values.__dict__()
        self.__values__ = values
        self.__update_cbs__ = []
        self.__updating__ = False

    def __setattr__(self, key, value):
        if key == "__table__" or key == "__values__" or key == "__update_cbs__" or key == "__updating__":
            super().__setattr__(key, value)
        else:
            self.update(**{key: value})
            self.__values__[key] = value

    def __getattr__(self, key):
        if key == "__table__" or key == "__values__" or key == "__update_cbs__" or key == "__updating__":
            return super().__getattr__(key)
        if key.startswith("edit_no_local"):
            return EditableRowItem(self.__values__[key[11:]],
                                   onchange=lambda value: self.update(local_change=False, **{key[11:]: value}),
                                   local_change=False)
        if key.startswith("edit_"):
            real_key = key[5:]
            e = EditableRowItem(self.__values__[real_key],
                                   onchange=lambda value: self.update(local_change=LOCAL_CHANGE, **{real_key: value}),
                                   local_change=LOCAL_CHANGE)
            if LOCAL_CHANGE:
                self.__update_cbs__.append(lambda old, new: e.local_updated(value=new[real_key]))
            return e
        if key.startswith("view_"):
            real_key = key[5:]
            if LOCAL_CHANGE:
                v = Value(self.__values__[real_key])
                self.__update_cbs__.append(lambda old, new: v.set(new[real_key]))
                return v
            else:
                return self.__values__[real_key]
        return self.__values__[key]
    
    # equality is basd on self.__values__
    def __eq__(self, other):
        return self.__values__ == other.__values__
    
    def __getitem__(self, key):
        return self.__values__[key]
    
    def __setitem__(self, key, value):
        self.update(**{key: value})
        self.__values__[key] = value
    
    def __repr__(self):
        return f"<Row {self.__table__.name if hasattr(self.__table__, 'name') else ''} {self.__values__}>"
    
    def __str__(self):
        return f"<Row {self.__table__.name if hasattr(self.__table__, 'name') else ''} {self.__values__}>"
    
    def __iter__(self):
        return iter(self.__values__)
    
    def __dict__(self):
        return self.__values__
    
    def __hash__(self):
        return frozenset(self.__values__.items()).__hash__()

    def keys(self):
        return self.__values__.keys()
    
    def values(self):
        return self.__values__.values()
    
    def items(self):
        return self.__values__.items()
    
    def delete(self):
        print("deleting row", self)
        global IN_PROGRESS
        if IN_PROGRESS:
            raise Exception("Cannot delete row while in progress")
        IN_PROGRESS = True
        if not self.__table__:
            raise Exception(f"Row {self} is not attached to a table")
        t = time.time()
        self.__table__.delete(id=self.id)
        print(f"delete took {(time.time() - t) * 1000}ms for ", self)
        IN_PROGRESS = False
    
    def update(self, local_change=False, **values):
        old_values = self.__values__
        if local_change:
            self.__updating__ = True
            self.__values__ = {**self.__values__, **values}
            for cb in self.__update_cbs__:
                cb(old_values, self.__values__)
        if not self.__table__:
            raise Exception(f"Row {self} is not attached to a table")
        # print(f"Updating {self.__table__} with {self.__dict__()} and {values}")
        self.__table__.update(old_values, **values)
        self.__updating__ = False

    def delete_urlm(self, redirect=None):
        redirect_query = f"?redirect={redirect}" if redirect else ""
        return URLM(f"/{self.__table__.name}/{self.id}{redirect_query}", "delete")
    
    def update_urlm(self, redirect=None, **values):
        values_with_redirect = {**values, "redirect": redirect} if redirect else values
        query = f"?{urlencode(values_with_redirect)}" if values_with_redirect else ""
        return URLM(f"/{self.__table__.name}/{self.id}{query}", "patch")

class View:
    """
    View class represents a view on a database table.

    Attributes:
        db: The database object.
        row_table: The table object.
        update_cbs: List of callbacks for update events.
        insert_cbs: List of callbacks for insert events.
        delete_cbs: List of callbacks for delete events.
        reset_cbs: List of callbacks for reset events.
        unique_keys: List of unique keys for the table.
        is_bool: Dictionary indicating if a column is a boolean.
        row_table: The table object.
    """
    def __init__(self, db, row_table=None):
        """
        Initialize the View object.

        Args:
            db: The database object.
            row_table: The table object.
        """
        self.db = db        
        self.update_cbs = []
        self.insert_cbs = []
        self.delete_cbs = []
        self.reset_cbs = []
        self.unique_keys = []
        self.is_bool = None
        self.row_table = row_table
        if hasattr(self, 'parent'):
            self.update_cbs_ref = methodref(self.call_update_cbs)
            self.parent.update_cbs.append(self.update_cbs_ref)
            self.insert_cbs_ref = methodref(self.call_insert_cbs)
            self.parent.insert_cbs.append(self.insert_cbs_ref)
            self.delete_cbs_ref = methodref(self.call_delete_cbs)
            self.parent.delete_cbs.append(self.delete_cbs_ref)
            self.reset_cbs_ref = methodref(self.call_reset_cbs)
            self.parent.reset_cbs.append(self.reset_cbs_ref)
            self.name = self.parent.name

    
    def tohtml(self, valueobj):
        return self.db.tohtml(valueobj)
    
    def __html__(self):
        """
        Return the HTML representation of the view in an HTML table.

        Returns:
            str: The HTML representation of the view in an HTML table.
        """
        r = self.tohtml(self)
        while hasattr(r, "__html__"):
            r = r.__html__()
        return r
    
    def name_or_query(self):
        """
        Return the name of the view if it has one, otherwise return the query.

        Returns:
            str: The name or query of the view.
        """
        return self.name if hasattr(self, 'name') else self.query
        
    def listen(self):
        """
        Listen for and print insert, update, and delete events on the view.
        """
        self.insert_cbs.append(lambda values: print(f"Inserted {values} into {self.name_or_query()}"))
        self.update_cbs.append(lambda old, new: print(f"Updated {old} to {new} in {self.name_or_query()}"))
        self.delete_cbs.append(lambda values: print(f"Deleted {values} from {self.name_or_query()}"))
    
    def unlisten(self):
        """
        Stop listening for insert, update, and delete events on the view.
        """
        self.insert_cbs.pop()
        self.update_cbs.pop()
        self.delete_cbs.pop()

    def maybe_to_bool(self, values):
        return [bool(v) if self.is_bool and self.is_bool[col] and (v == 1 or v == 0) else v for col, v in enumerate(values)]

    def is_bool_col(self, col):
        if col in self.columns:
            return self.is_bool[self.columns.index(col)]
        else:
            return False

    def back_from_bool(self, values):
        return {col: int(v) if isinstance(v, bool) else 0 if v=="False" and self.is_bool_col(col) else 1 if v=="True" and self.is_bool_col(col) else v for col, v in values.items()}

    def __iter__(self):
        """
        Iterate over the rows in the view.

        Returns:
            iterator: An iterator over the rows in the view.
        """
        return map(
            lambda values: Row(
                {col: val for col, val in zip(self.columns, self.maybe_to_bool(values))},
                self
            ),
            self.db.execute(self.query)
        )

    def fetchall(self):
        """
        Fetch all rows from the view.

        Returns:
            list: A list of rows from the view.
        """
        return self.db.execute(self.query)
    
    def fetchone(self, **values):
        """
        Fetch one row from the view based on the given values.

        Args:
            values: Dictionary of column values to filter the row.

        Returns:
            Row: The row matching the given values, or None if no row is found.
        """
        query = f"SELECT * FROM ({self.query}) {'WHERE' if values else ''} {', '.join([f'{k}=?' for k in values])}"
        row = self.db.fetchone(query, tuple(values.values()))
        return Row({col: val for col, val in zip(self.columns, self.maybe_to_bool(row))}, self) if row else None
    
    def __repr__(self):
        return f"<{type(self).__name__}({self.columns}) {self.query}>"
    
    def __str__(self):
        return f"<{type(self).__name__}({self.columns}) {self.query}>"
    
    def call_insert_cbs(self, values):
        for cb in self.insert_cbs:
            cb(values)
    
    def call_update_cbs(self, old, new):
        for cb in self.update_cbs:
            cb(old, new)
    
    def call_delete_cbs(self, values):
        for cb in self.delete_cbs:
            cb(values)

    def call_reset_cbs(self):
        self.reset()
        for cb in self.reset_cbs:
            cb()

    def __del__(self):
        if hasattr(self, 'parent'):
            self.parent.update_cbs.remove(self.update_cbs_ref)
            self.parent.insert_cbs.remove(self.insert_cbs_ref)
            self.parent.delete_cbs.remove(self.delete_cbs_ref)
            self.parent.reset_cbs.remove(self.reset_cbs_ref)
    
    def select(self, **colexprs):
        """
        Create a new Select object with the current view as the parent.

        Args:
            colexprs: Dictionary of column expressions.

        Returns:
            Select: A new Select object.
        """
        return Select(self, **colexprs)

    def where(self, main=None, **where):
        """
        Create a new WHERE clause with main and additional column conditions.
        """
        return Where(self, main, **where)
    
    def union(self, parent2):
        """
        Create a UNION query with the current view and another view.
        """
        return SQLUnion(self, parent2)
    
    def union_all(self, parent2):
        """
        Create a UNION ALL query with the current view and another view.
        """
        return UnionAll(self, parent2)
    
    def join(self, parent2, left_outer=False, right_outer=False, left_name='a', right_name='b', **on):
        return Join(self, parent2, left_outer, right_outer, left_name, right_name, **on)
    
    def distinct(self):
        return Distinct(self)
    
    def group_by(self, *columns, **aggregations):
        return GroupBy(self, *columns, **aggregations)
    
    def count(self):
        return ColumnValue(self.group_by(count="COUNT(*)"), "count")
    
    def only(self):
        return RowValue(self)
    
    def sum(self, column):
        return ColumnValue(self.group_by(sum=f"SUM({column})"), "sum")
    
    def avg(self, column):
        return ColumnValue(self.group_by(avg=f"AVG({column})"), "avg")
    
    def min(self, column):
        return ColumnValue(self.group_by(min=f"MIN({column})"), "min")
    
    def max(self, column):
        return ColumnValue(self.group_by(max=f"MAX({column})"), "max")
    
    def sort(self, order_by=None, limit=None, offset=None):
        return Sort(self, order_by, limit, offset)

    # Reset is called when a parent query has changed. Columns can't change for now.
    def reset(self):
        pass

    def on_delete(self, cb):
        f = lambda row: cb(Row(row, self))
        self.delete_cbs.append(f)
        return lambda: self.delete_cbs.remove(f)

    def on_insert(self, cb):
        f = lambda row: cb(Row(row, self))
        self.insert_cbs.append(f)
        return lambda: self.insert_cbs.remove(f)

    def on_update(self, cb):
        f = lambda old, new: cb(Row(old, self), Row(new, self))
        self.update_cbs.append(f)
        return lambda: self.update_cbs.remove(f)

    def on_reset(self, cb):
        f = cb
        self.reset_cbs.append(f)
        return lambda: self.reset_cbs.remove(f)

    def delete(self, id):
        if self.parent:
            self.parent.delete(id)
        else:
            raise Exception("Not implemented")
        
    def update(self, where, **values):
        raise Exception("Not implemented")

    def print(self):
        # columns
        print(self.columns)
        print("--------------------------------")
        for row in self.db.execute(self.query):
            print(row)
        print("--------------------------------")

# class Reactive(View):
#     def __init__(self, parent):
#         super().__init__(parent.db, row_table=parent.row_table)
    
#     def set_parent(self, parent):
#         self.parent.update_cbs.remove(self.call_update_cbs)
#         self.parent.insert_cbs.remove(self.call_insert_cbs)
#         self.parent.delete_cbs.remove(self.call_delete_cbs)
#         self.parent.reset_cbs.remove(self.call_reset_cbs)
#         self.parent = parent
#         self.db = parent.db
#         self.parent.update_cbs.append(self.call_update_cbs)
#         self.parent.insert_cbs.append(self.call_insert_cbs)
#         self.parent.delete_cbs.append(self.call_delete_cbs)
#         self.parent.reset_cbs.append(self.call_reset_cbs)
#         self.reset()
#         for cb in self.reset_cbs:
#             cb()

EXPERIMENTAL_OUTER_JOIN = True

class Join(View):
    def __init__(self, parent, parent2, left_outer=False, right_outer=False, left_name='a', right_name='b', **on):
        if left_outer and right_outer and not EXPERIMENTAL_OUTER_JOIN:
            raise Exception("Full outer join not supported yet. Set EXPERIMENTAL_OUTER_JOIN to True to enable it, but it's still buggy.")
        self.parent = parent
        self.parent2 = parent2
        super().__init__(parent.db, row_table=parent.row_table)
        self.update_cbs_ref2 = methodref(self.call_update_cbs2)
        self.insert_cbs_ref2 = methodref(self.call_insert_cbs2)
        self.delete_cbs_ref2 = methodref(self.call_delete_cbs2)
        parent2.update_cbs.append(self.update_cbs_ref2)
        parent2.insert_cbs.append(self.insert_cbs_ref2)
        parent2.delete_cbs.append(self.delete_cbs_ref2)
        on_values = [x.upper() for x in on.values()]
        right_prefix = f"{right_name}." if right_name else ""
        left_prefix = f"{left_name}." if left_name else ""
        self.column_selectors = [f"{left_prefix}{x}"for x in parent.columns] + [f"{right_prefix}{x}" for x in parent2.columns if x.upper() not in on_values]
        self.columns = parent.columns[:]
        for col in parent2.columns:
            if col.upper() not in on_values:
                i = 1
                while f"{col}{i if i > 1 else ''}" in self.columns:
                  i += 1
                self.columns.append(f"{col}{i if i > 1 else ''}")
        if left_outer or right_outer:
            self.column_selectors =  [f"{left_prefix}{x}" for x in parent.columns] + [f"{right_prefix}{x}" for x in parent2.columns]
            self.columns = parent.columns[:]
            for col in parent2.columns:
                i = 1
                while f"{col}{i if i > 1 else ''}" in self.columns:
                  i += 1
                self.columns.append(f"{col}{i if i > 1 else ''}")
        self.columns_with_selectors = [f"{selector} AS {col}" for selector, col in zip(self.column_selectors, self.columns)]
        self.left_outer = left_outer
        self.right_outer = right_outer
        self.on = on
        self.left_prefix = left_prefix
        self.right_prefix = right_prefix
        self.left_name = left_name
        self.right_name = right_name
        self.reset()
    
    def reset(self):
        as_left_name = f"as {self.left_name}" if self.left_name else ""
        as_right_name = f"as {self.right_name}" if self.right_name else ""
        join_type = "FULL OUTER JOIN" if self.left_outer and self.right_outer else (
                    "LEFT OUTER JOIN" if self.left_outer else ("RIGHT OUTER JOIN" if self.right_outer else "JOIN"))
        on_clause = ""
        if self.on:
            on_clause = f"ON {', '.join([f'{self.left_prefix}{k}={self.right_prefix}{v}' for k, v in self.on.items()])}"

        self.query = f"SELECT {', '.join(self.columns_with_selectors)} FROM ({self.parent.query}) {as_left_name} {join_type} ({self.parent2.query}) {as_right_name} {on_clause}"
    
    def call_insert_cbs(self, values):
        where_clause = f"WHERE {', '.join([f'{self.on[k]}=?' for k in self.on.keys()])}" if self.on else ""
        parent2_matches = self.db.fetchall(f"SELECT * FROM ({self.parent2.query}) {where_clause}", tuple(values[k] for k in self.on.keys()))
        values_array = [values[col] for col in self.parent.columns]
        left_matches_after_insert = None
        if self.right_outer:
            where_clause = f"WHERE {', '.join([f'{k}=?' for k in self.on.values()])}" if self.on else ""
            left_matches_after_insert = len(self.db.fetchall(f"SELECT * FROM ({self.parent.query}) {where_clause} LIMIT 2", tuple(values[self.on[k]] for k in self.on.keys())))
        for match in parent2_matches:
            if self.left_outer or self.right_outer:
                joined_values_array = values_array + list(match)
            else:
                joined_values_array = values_array + [match[i] for i, col in enumerate(self.parent2.columns) if col not in self.on.values()]
            joined_values = {col: joined_values_array[i] for i, col in enumerate(self.columns)}
            for cb in self.insert_cbs:
                cb(joined_values)
            if self.right_outer and left_matches_after_insert == 1:
                joined_values_array = [None for _ in self.parent.columns] + list(match)
                joined_values = {col: joined_values_array[i] for i, col in enumerate(self.columns)}
                for cb in self.delete_cbs:
                    cb(joined_values)
        if not parent2_matches and self.left_outer:
            joined_values_array = values_array + [None for _ in self.parent2.columns]
            joined_values = {col: joined_values_array[i] for i, col in enumerate(self.columns)}
            for cb in self.insert_cbs:
                cb(joined_values)

    def call_delete_cbs(self, values):
        # Query to find the matching rows in parent2
        where_clause = f"WHERE {', '.join([f'{k}=?' for k in self.on.keys()])}" if self.on else ""
        parent2_matches = self.db.execute(f"SELECT * FROM ({self.parent2.query}) {where_clause}", tuple(values[k] for k in self.on.keys()))
        values_array = [values[col] for col in self.parent.columns]
        for match in parent2_matches:
            if self.left_outer or self.right_outer:
                joined_values_array = values_array + list(match)
            else:
                joined_values_array = values_array + [match[idx] for idx, col in enumerate(self.parent2.columns) if col not in self.on.values()]
            joined_values = {col: joined_values_array[i] for i, col in enumerate(self.columns)}
            for cb in self.delete_cbs:
                cb(joined_values)
            if self.right_outer:
                # If right outer and there's no match for the joined part anymore in the left table,
                # Nones are inserted [ update should be probably emitted instead ]
                where_clause = f"WHERE {', '.join([f'{k}=?' for k in self.on.keys()])}" if self.on else ""
                parent1_matches = self.db.fetchone(f"SELECT * FROM ({self.parent.query}) {where_clause}", tuple(values[k] for k in self.on.keys()))
                if not parent1_matches:
                    joined_values_array = [None for _ in self.parent.columns] + list(match)
                    joined_values = {col: joined_values_array[i] for i, col in enumerate(self.columns)}
                    for cb in self.insert_cbs:
                        cb(joined_values)
        if not parent2_matches and self.left_outer:
            joined_values_array = [values[col] for col in self.parent.columns] + [None for _ in self.parent2.columns]
            joined_values = {col: joined_values_array[i] for i, col in enumerate(self.columns)}
            for cb in self.delete_cbs:
                cb(joined_values)

    def call_update_cbs(self, old, new):
        # Check if key changed:
        old_key = tuple([old[k] for k in self.on.keys()])
        new_key = tuple([new[k] for k in self.on.keys()])
        if old_key == new_key:
            # Handle update by checking in the right table
            where_clause = f"WHERE {', '.join([f'{k}=?' for k in self.on.keys()])}" if self.on else ""
            parent2_matches = self.db.fetchall(f"SELECT * FROM ({self.parent2.query}) {where_clause}", old_key)
            old_array = [old[col] for col in self.parent.columns] 
            new_array = [new[col] for col in self.parent.columns]
            for match in parent2_matches:
                if self.left_outer or self.right_outer:
                    old_joined_array = old_array + list(match)
                    new_joined_array = new_array + list(match)
                else:
                    old_joined_array = old_array + [match[idx] for idx, col in enumerate(self.parent2.columns) if col not in self.on.values()]
                    new_joined_array = new_array + [match[idx] for idx, col in enumerate(self.parent2.columns) if col not in self.on.values()]
                old_joined_values = {col: old_joined_array[i] for i, col in enumerate(self.columns)}
                new_joined_values = {col: new_joined_array[i] for i, col in enumerate(self.columns)}
                for cb in self.update_cbs:
                    cb(old_joined_values, new_joined_values)
            if not parent2_matches and self.right_outer:
                old_joined_array = old_array + [None for _ in self.parent2.columns]
                old_joined_values = {col: old_joined_array[i] for i, col in enumerate(self.columns)}
                new_joined_array = new_array + [None for _ in self.parent2.columns]
                new_joined_values = {col: new_joined_array[i] for i, col in enumerate(self.columns)}
                for cb in self.update_cbs:
                    cb(old_joined_values, new_joined_values)
            return
        if self.left_outer or self.right_outer:
            # Just call insert and delete for now
            self.call_insert_cbs(new)
            self.call_delete_cbs(old)
            return
        # First, handle the deletion of the old joined row
        where_clause = f"WHERE {', '.join([f'{k}=?' for k in self.on.keys()])}" if self.on else ""
        parent2_matches = self.db.fetchall(f"SELECT * FROM ({self.parent2.query}) {where_clause}", tuple(old[k] for k in self.on.keys()))
        to_delete = []
        old_array = [old[col] for col in self.parent.columns]
        new_array = [new[col] for col in self.parent.columns]

        for match in parent2_matches:
            if self.left_outer or self.right_outer:
                old_joined_array = old_array + list(match)
            else:
                old_joined_array = old_array + [match[idx] for idx, col in enumerate(self.parent2.columns) if col not in self.on.values()]
            to_delete.append(old_joined_array)

        # Now, handle the insertion of the new joined row
        where_clause = f"WHERE {', '.join([f'{k}=?' for k in self.on.keys()])}" if self.on else ""
        parent2_matches = self.db.fetchall(f"SELECT * FROM ({self.parent2.query}) {where_clause}", tuple(new[k] for k in self.on.keys()))
        to_insert = []
        for match in parent2_matches:
            if self.left_outer or self.right_outer:
                new_joined_array = new_array + list(match)
            else:
                new_joined_array = new_array + [match[idx] for idx, col in enumerate(self.parent2.columns) if col not in self.on.values()]
            to_insert.append(new_joined_array)
        
        if len(to_insert) == 1 and len(to_delete) == 1:
            to_delete_dict = {self.columns[i]: to_delete[0][i] for i in range(len(self.columns))}   
            to_insert_dict = {self.columns[i]: to_insert[0][i] for i in range(len(self.columns))}
            for cb in self.update_cbs:
                cb(to_delete_dict, to_insert_dict)
        else:
            for values in to_insert:
                values_dict = {self.columns[i]: values[i] for i in range(len(self.columns))}
                for cb in self.insert_cbs:
                    cb(values_dict)
            for values in to_delete:
                values_dict = {self.columns[i]: values[i] for i in range(len(self.columns))}
                for cb in self.delete_cbs:
                    cb(values_dict)

    def call_insert_cbs2(self, values):
        # Query to find the matching rows in parent1
        where_clause = f"WHERE {', '.join([f'{k}=?' for k in self.on.keys()])}" if self.on else ""
        parent1_matches = self.db.fetchall(f"SELECT * FROM ({self.parent.query}) {where_clause}", tuple(values[self.on[k]] for k in self.on.keys()))
        values_array = [values[col] for col in self.parent2.columns]
        right_matches_after_insert = None
        if self.left_outer:
            where_clause = f"WHERE {', '.join([f'{k}=?' for k in self.on.values()])}" if self.on else ""
            right_matches_after_insert = len(self.db.fetchall(f"SELECT * FROM ({self.parent2.query}) {where_clause} LIMIT 2", tuple(values[self.on[k]] for k in self.on.keys())))
        for match in parent1_matches:
            if self.left_outer or self.right_outer:
                joined_values_array = [match[idx] for idx, col in enumerate(self.parent.columns)] + values_array
            else:
                joined_values_array = [match[idx] for idx, col in enumerate(self.parent.columns)] + [values[col] for col in self.parent2.columns if col not in self.on.values()]
            joined_values = {col: joined_values_array[i] for i, col in enumerate(self.columns)}
            for cb in self.insert_cbs:
                cb(joined_values)
            if self.left_outer and right_matches_after_insert == 1:
                # delete with None on the right side
                joined_values_array = [match[idx] for idx, col in enumerate(self.parent.columns)] + [None for _ in self.parent2.columns]
                joined_values = {col: joined_values_array[i] for i, col in enumerate(self.columns)}
                for cb in self.delete_cbs:
                    cb(joined_values)
        if not parent1_matches and self.right_outer:
            joined_values_array = [None for _ in self.parent.columns] + values_array
            # None for left outer
            joined_values = {col: joined_values_array[i] for i, col in enumerate(self.columns)}
            for cb in self.insert_cbs:
                cb(joined_values)

    def call_delete_cbs2(self, values):
        # Query to find the matching rows in parent1
        where_clause = f"WHERE {', '.join([f'{k}=?' for k in self.on.keys()])}" if self.on else ""
        parent1_matches = self.db.fetchall(f"SELECT * FROM ({self.parent.query}) {where_clause}", tuple(values[self.on[k]] for k in self.on.keys()))
        values_array = [values[col] for col in self.parent2.columns]
        for match in parent1_matches:
            if self.left_outer or self.right_outer:
                joined_values_array = [match[idx] for idx, col in enumerate(self.parent.columns)] + values_array
            else:
                joined_values_array = [match[idx] for idx, col in enumerate(self.parent.columns)] + [values[col] for col in self.parent2.columns if col not in self.on.values()]
            joined_values = {col: joined_values_array[i] for i, col in enumerate(self.columns)}
            for cb in self.delete_cbs:
                cb(joined_values)
            if self.left_outer:
                where_clause = f"WHERE {', '.join([f'{k}=?' for k in self.on.keys()])}" if self.on else ""
                parent2_matches = self.db.fetchone(f"SELECT * FROM ({self.parent2.query}) {where_clause}", tuple(values[k] for k in self.on.keys()))
                if not parent2_matches:
                    joined_values_array = list(match) + [None for _ in self.parent2.columns]
                    joined_values = {col: joined_values_array[i] for i, col in enumerate(self.columns)}
                    for cb in self.insert_cbs:
                        cb(joined_values)
              
        if not parent1_matches and self.right_outer:
            joined_values_array = [None for _ in self.parent.columns] + values_array
            joined_values = {col: joined_values_array[i] for i, col in enumerate(self.columns)}
            for cb in self.delete_cbs:
                cb(joined_values)
  
    def call_update_cbs2(self, old, new):
        old_key = tuple([old[k] for k in self.on.keys()])
        new_key = tuple([new[k] for k in self.on.keys()])
        if old_key == new_key:
            # Handle update by checking in the right table
            where_clause = f"WHERE {', '.join([f'{k}=?' for k in self.on.keys()])}" if self.on else ""
            parent_matches = self.db.fetchall(f"SELECT * FROM ({self.parent.query}) {where_clause}", old_key)
            old_array = [old[col] for col in self.parent2.columns] 
            new_array = [new[col] for col in self.parent2.columns]
            for match in parent_matches:
                if self.left_outer or self.right_outer:
                    old_joined_array = list(match) + old_array
                    new_joined_array = list(match) + new_array
                else:
                    old_joined_array = list(match) + [old[col] for col in self.parent2.columns if col not in self.on.values()]
                    new_joined_array = list(match) + [new[col] for col in self.parent2.columns if col not in self.on.values()]
                old_joined_values = {col: old_joined_array[i] for i, col in enumerate(self.columns)}
                new_joined_values = {col: new_joined_array[i] for i, col in enumerate(self.columns)}
                for cb in self.update_cbs:
                    cb(old_joined_values, new_joined_values)
            if not parent_matches and self.left_outer:
                old_joined_array = [None for _ in self.parent.columns] + old_array
                old_joined_values = {col: old_joined_array[i] for i, col in enumerate(self.columns)}
                new_joined_array = [None for _ in self.parent.columns] + new_array
                new_joined_values = {col: new_joined_array[i] for i, col in enumerate(self.columns)}
                for cb in self.update_cbs:
                    cb(old_joined_values, new_joined_values)
            return
        if self.left_outer or self.right_outer:
            # Just call insert and delete for now
            self.call_insert_cbs2(new)
            self.call_delete_cbs2(old)
            return
        # First, handle the deletion of the old joined row
        where_clause = f"WHERE {', '.join([f'{self.on[k]}=?' for k in self.on.keys()])}" if self.on else ""
        parent1_matches = self.db.fetchall(f"SELECT * FROM ({self.parent.query}) {where_clause}", tuple(old[self.on[k]] for k in self.on.keys()))
        to_delete = []
        to_insert = []
        old_array = [old[col] for col in self.parent2.columns]
        new_array = [new[col] for col in self.parent2.columns]
        for match in parent1_matches:
            if self.left_outer or self.right_outer:
                old_joined_array = [match[idx] for idx, col in enumerate(self.parent.columns)] + old_array
            else:
                old_joined_array = [match[idx] for idx, col in enumerate(self.parent.columns)] + [old[col] for col in self.parent2.columns if col not in self.on.values()]
            to_delete.append(old_joined_array)

        # Now, handle the insertion of the new joined row
        where_clause = f"WHERE {', '.join([f'{k}=?' for k in self.on.keys()])}" if self.on else ""
        parent1_matches = self.db.fetchall(f"SELECT * FROM ({self.parent.query}) {where_clause}", tuple(new[self.on[k]] for k in self.on.keys()))
        for match in parent1_matches:
            if self.left_outer or self.right_outer:
                new_joined_array = [match[idx] for idx, col in enumerate(self.parent.columns)] + new_array
            else:
                new_joined_array = [match[idx] for idx, col in enumerate(self.parent.columns)] + [new[col] for col in self.parent2.columns if col not in self.on.values()]
            to_insert.append(new_joined_array)
        if len(to_insert) == 1 and len(to_delete) == 1:
            to_delete_dict = {self.columns[i]: to_delete[0][i] for i in range(len(self.columns))}   
            to_insert_dict = {self.columns[i]: to_insert[0][i] for i in range(len(self.columns))}
            for cb in self.update_cbs:
                cb(to_delete_dict, to_insert_dict)
        else:
            for values in to_insert:
                values_dict = {self.columns[i]: values[i] for i in range(len(self.columns))}
                for cb in self.insert_cbs:
                    cb(values_dict)
            for values in to_delete:
                values_dict = {self.columns[i]: values[i] for i in range(len(self.columns))}
                for cb in self.delete_cbs:
                    cb(values_dict)

    def update(self, where, **values):
        # check if all values are present in the parent columns
        is_parent1 = True
        for col in values:
            if col not in self.parent.columns:
                is_parent1 = False
        if is_parent1:
            self.parent.update(where, **values)
        else:
            raise ValueError(
                f"Join.update: Can not update column {col} because it does not exist in the parent table {self.parent.name}, values: {values}, parent.columns: {self.parent.columns}. Parent2 not yet implemented.")

    def __del__(self):
        self.parent2.update_cbs.remove(self.update_cbs_ref2)
        self.parent2.insert_cbs.remove(self.insert_cbs_ref2)
        self.parent2.delete_cbs.remove(self.delete_cbs_ref2)
        super().__del__()

class Distinct(View):
    def __init__(self, parent):
        self.parent = parent
        super().__init__(parent.db, row_table=parent.row_table)
        self.columns = parent.columns
        self.query = f"SELECT DISTINCT * FROM ({self.parent.query})"
        self.value_hashes_counts = {}
        for row in self.db.fetchall(self.query):
            self.value_hashes_counts[tuple(row).__hash__()] = self.value_hashes_counts.get(tuple(row).__hash__(), 0) + 1

    def call_insert_cbs(self, values):
        transformed_values_values = [values[col] for col in self.columns]
        value_hash = tuple(transformed_values_values).__hash__()
        count = self.value_hashes_counts.get(value_hash, 0)
        self.value_hashes_counts[value_hash] = count + 1
        if count == 0:
            for cb in self.insert_cbs:
                cb(values)

    def call_update_cbs(self, old, new):
        transformed_old_values_values = [old[col] for col in self.columns]
        transformed_new_values_values = [new[col] for col in self.columns]
        old_value_hash = tuple(transformed_old_values_values).__hash__()
        new_value_hash = tuple(transformed_new_values_values).__hash__()
        old_count = self.value_hashes_counts.get(old_value_hash, 0)
        new_count = self.value_hashes_counts.get(new_value_hash, 0)
        self.value_hashes_counts[old_value_hash] = old_count - 1
        self.value_hashes_counts[new_value_hash] = new_count + 1
        if old_count == 1:
            if new_count == 0:
                for cb in self.update_cbs:
                    cb(new)
            else:
                for cb in self.delete_cbs:
                    cb(old)
        elif new_count == 0:
            for cb in self.insert_cbs:
                cb(new)

    def call_delete_cbs(self, values):
        transformed_values_values = [values[col] for col in self.columns]
        value_hash = tuple(transformed_values_values).__hash__()
        count = self.value_hashes_counts.get(value_hash, 0)
        if count == 0:
            raise Exception(f"Can not delete a non-existing row: {values}")
        self.value_hashes_counts[value_hash] = count - 1
        if count == 1:
            for cb in self.delete_cbs:
                cb(values)

def value_to_sql(value):
    if value is None:
        return "NULL"
    elif isinstance(value, str):
        return f"'{value}'"
    else:
        return str(value)
    
class Select(View):
    def __init__(self, parent, **colexprs: Union[str, Type]) -> Optional[str]:
        self.parent = parent
        rowtable = None
        self.mirrors_id = False
        if colexprs.get("id") == True or colexprs.get("id") == "id":
            rowtable = parent.row_table
            self.mirrors_id = True
        super().__init__(parent.db, row_table=rowtable)
        self.columns = [col for col in colexprs]
        column_parts = [col if expr == True else f"{expr} AS {col}" for col, expr in colexprs.items()]
        self.select_query = f"SELECT {', '.join(column_parts)}"
        self.query = f"SELECT {', '.join(column_parts)} FROM ({self.parent.query})"
    
    def call_insert_cbs(self, values):
        transformed_values_values = self.db.fetchone(self.select_query + f" FROM (SELECT " + ", ".join([f"{value_to_sql(value)} AS {col}" for col, value in values.items()]) + ")")
        transformed_values = {k: v for k, v in zip(self.columns, transformed_values_values)}
        for cb in self.insert_cbs:
            cb(transformed_values)

    def call_update_cbs(self, old, new):
        transformed_old_values_values = self.db.fetchone(self.select_query + f" FROM (SELECT " + ", ".join([f"{value_to_sql(value)} AS {col}" for col, value in old.items()]) + ")")
        transformed_old_values = {k: v for k, v in zip(self.columns, transformed_old_values_values)}
        transformed_new_values_values = self.db.fetchone(self.select_query + f" FROM (SELECT " + ", ".join([f"{value_to_sql(value)} AS {col}" for col, value in new.items()]) + ")")
        transformed_new_values = {k: v for k, v in zip(self.columns, transformed_new_values_values)}
        for cb in self.update_cbs:
            cb(transformed_old_values, transformed_new_values)

    def call_delete_cbs(self, values):
        transformed_values_values = self.db.fetchone(self.select_query + f" FROM (SELECT " + ", ".join([f"{value_to_sql(value)} AS {col}" for col, value in values.items()]) + ")")
        transformed_values = {k: v for k, v in zip(self.columns, transformed_values_values)}
        for cb in self.delete_cbs:
            cb(transformed_values)
    
    def delete(self, id):
        if self.mirrors_id:
            self.parent.delete(id=id)
        else:
            raise ValueError("Can not delete without id in select / parent for query " + self.query)
        
    def update(self, where, **values):
        if self.mirrors_id and 'id' in where:
            self.parent.update(where, **values)
        else:
            raise ValueError("Can not update without id in select + where for query " + self.query)    


class Where(View):
    def __init__(self, parent, main=None, **where: Union[str, Type]) -> Optional[str]:
        self.parent = parent
        super().__init__(parent.db, row_table=parent.row_table)
        self.columns = parent.columns
        self.set_filter(main=main, **where)
    
    def is_where_true(self, values):
        if not self.main:
            # just check vlaues in where
            for col, value in self._where.items():
                if values[col] != value:
                    return False
            return True

        placeholders = ', '.join([f"? as '{col}'" for col in values.keys()])
        result = self.db.fetchone(f"SELECT * FROM (SELECT {placeholders}) {self.where_query}", tuple(values.values()))
        return result
    
    def call_insert_cbs(self, values):
        if self.is_where_true(values):
            for cb in self.insert_cbs:
                cb(values)
    
    def call_delete_cbs(self, values):
        if self.is_where_true(values):
            for cb in self.delete_cbs:
                cb(values)
    
    def call_update_cbs(self, old, new):
        old_exists = self.is_where_true(old)
        new_exists = self.is_where_true(new)
        if old_exists and new_exists:
            for cb in self.update_cbs:
                cb(old, new)
        elif old_exists:
            for cb in self.delete_cbs:
                cb(old)
        elif new_exists:
            for cb in self.insert_cbs:
                cb(new)

    def set_filter(self, main=None, **where):
        self.main = main
        self._where = where
        self.where_query = f"{'WHERE' if main or where else ''} {('(' + main + ')' if main else '') + (' AND ' if main and where else '')} {' AND '.join([f'{col}={value.__repr__()}' for col, value in where.items()])}"
        self.reset()
        for cb in self.reset_cbs:
            cb()

    def reset(self):
        self.query = f"SELECT * FROM ({self.parent.query}) {self.where_query}"

    def update(self, where, **values):
        print(f"Where: Updating {self.parent.name} with {where} and {values}")
        if self.main is not None or "id" in where:
            self.parent.update({**where, **self._where}, **values)
        else:
            raise ValueError(f"Can not update without id in where for self.query {self.query}, self.main: {self.main}, self.where: {self.where}, where: {where}, values: {values}")

class UnionAll(View):
    def __init__(self, parent, parent2):
        self.parent = parent
        self.parent2 = parent2
        super().__init__(parent.db, row_table=parent.row_table)
        self.columns = parent.columns
        self.query = f"{self.parent.query} UNION ALL {self.parent2.query}"
        # These method refs are creaated by View constructor for self.parent, here they are reused
        parent2.update_cbs.append(self.update_cbs_ref)
        parent2.insert_cbs.append(self.insert_cbs_ref)
        parent2.delete_cbs.append(self.delete_cbs_ref)

    def call_insert_cbs(self, values):
        for cb in self.insert_cbs:
            cb(values)

    def call_delete_cbs(self, values):
        for cb in self.delete_cbs:
            cb(values)
    
    def call_update_cbs(self, old, new):
        for cb in self.update_cbs:
            cb(old, new)

    def __del__(self):
        self.parent2.update_cbs.remove(self.update_cbs_ref)
        self.parent2.insert_cbs.remove(self.insert_cbs_ref)
        self.parent2.delete_cbs.remove(self.delete_cbs_ref)

class SQLUnion(View):  # typing.Union is used too widely for this class to be named Union
    def __init__(self, parent, parent2) -> Optional[str]:
        self.parent = parent
        self.parent2 = parent2
        super().__init__(parent.db, row_table=parent.row_table)
        self.update_cbs_ref2 = methodref(self.call_update_cbs2)
        self.insert_cbs_ref2 = methodref(self.call_insert_cbs2)
        self.delete_cbs_ref2 = methodref(self.call_delete_cbs2)
        parent2.update_cbs.append(self.update_cbs_ref2)
        parent2.insert_cbs.append(self.insert_cbs_ref2)
        parent2.delete_cbs.append(self.delete_cbs_ref2)
        if sorted(map(lambda c: c.upper(), parent.columns)) != sorted(map(lambda c: c.upper(), parent2.columns)):
            raise ValueError("Union views must have the same columns.")
        self.columns = parent.columns
        self.query = f"{self.parent.query} UNION {self.parent2.query}"
        self.value_hashes_counts = {}
        for row in self.db.fetchall(self.parent.query):
            self.value_hashes_counts[tuple(row).__hash__()] = self.value_hashes_counts.get(tuple(row).__hash__(), 0) + 1
        for row in self.db.fetchall(self.parent2.query):
            self.value_hashes_counts[tuple(row).__hash__()] = self.value_hashes_counts.get(tuple(row).__hash__(), 0) + 1
    
    def call_insert_cbs(self, values):
        transformed_values_values = [values[col] for col in self.columns]
        value_hash = tuple(transformed_values_values).__hash__()
        count = self.value_hashes_counts.get(value_hash, 0)
        self.value_hashes_counts[value_hash] = count + 1
        if count == 0:
            for cb in self.insert_cbs:
                cb(values)
        
    def call_delete_cbs(self, values):
        transformed_values_values = [values[col] for col in self.columns]
        value_hash = tuple(transformed_values_values).__hash__()
        count = self.value_hashes_counts.get(value_hash, 0)
        if count == 0:
            raise Exception(f"Can not delete a non-existing row: {values}")
        self.value_hashes_counts[value_hash] = count - 1
        if count == 1:
            for cb in self.delete_cbs:
                cb(values)

    def call_update_cbs(self, old, new):
        transformed_old_values_values = [old[col] for col in self.columns]
        transformed_new_values_values = [new[col] for col in self.columns]
        old_value_hash = tuple(transformed_old_values_values).__hash__()
        new_value_hash = tuple(transformed_new_values_values).__hash__()
        old_count = self.value_hashes_counts.get(old_value_hash, 0)
        new_count = self.value_hashes_counts.get(new_value_hash, 0)
        self.value_hashes_counts[old_value_hash] = old_count - 1
        self.value_hashes_counts[new_value_hash] = new_count + 1
        if old_count == 1:
            if new_count == 0:
                for cb in self.update_cbs:
                    cb(new)
            else:
                for cb in self.delete_cbs:
                    cb(old)
        elif new_count == 0:
            for cb in self.insert_cbs:
                cb(new)
    
    def call_insert_cbs2(self, values):
        # check parent
        if not self.db.fetchone(f"SELECT * FROM ({self.parent.query} WHERE {', '.join([f'{k}=?' for k in values])};", tuple(values.values())):
            for cb in self.insert_cbs:
                cb(values)
    
    def call_delete_cbs2(self, values):
        # check parent
        if not self.db.fetchone(f"SELECT * FROM ({self.parent.query} WHERE {', '.join([f'{k}=?' for k in values])};", tuple(values.values())):
            for cb in self.delete_cbs:
                cb(values)

    def call_update_cbs2(self, old, new):
        # check parent
        old_exists = self.db.fetchone(f"SELECT * FROM ({self.parent.query} WHERE {', '.join([f'{k}=?' for k in old])};", tuple(old.values()))
        new_exists = self.db.fetchone(f"SELECT * FROM ({self.parent.query} WHERE {', '.join([f'{k}=?' for k in new])};", tuple(new.values()))
        if (not old_exists) and (not new_exists):
            for cb in self.update_cbs:
                cb(old, new)
        elif not old_exists:
            for cb in self.delete_cbs:
                cb(old)
        elif not new_exists:
            for cb in self.insert_cbs:
                cb(new)
    
    def __del__(self):
        self.parent2.update_cbs.remove(self.update_cbs_ref2)
        self.parent2.insert_cbs.remove(self.insert_cbs_ref2)
        self.parent2.delete_cbs.remove(self.delete_cbs_ref2)

class Table(View):
    def __init__(self, db, _name: str, temp=False, **columns: Union[str, Type]):
        super().__init__(db, row_table=_name)
        # self.db = db
        self.temp = temp
        self.update_cbs_ref = methodref(self.call_update_cbs)
        self.insert_cbs_ref = methodref(self.call_insert_cbs)
        self.delete_cbs_ref = methodref(self.call_delete_cbs)
        self.db.update_cbs.append(self.update_cbs_ref)
        self.db.insert_cbs.append(self.insert_cbs_ref)
        self.db.delete_cbs.append(self.delete_cbs_ref)
        table_exists = self.db.fetchone(f"SELECT sql FROM sqlite_master WHERE type='table' AND name= ?;", (_name,))
        self.name = _name

        if not table_exists and not columns:
            raise ValueError(f"Table '{_name}' does not exist and no columns were provided.")

        # Construct the SQL command to create the table
        column_definitions = []
        self.is_bool = []
        for col, dtype in columns.items():
            column_definitions.append( (col, dtype.upper() if isinstance(dtype, str) else python_to_sqlite_type[dtype]))
            self.is_bool.append(dtype == bool)

        if table_exists:
            sql = table_exists[0]
            fa = self.db.fetchall(f"PRAGMA table_info({_name});")
            existing_columns = []
            for farow in fa:
                if farow[5]:
                    self.unique_keys.append([farow[1]])
                dtype = farow[2].upper() + (" NOT NULL" if farow[3] else "") + (" DEFAULT " + farow[4] if farow[4] else "") + (" PRIMARY KEY" if farow[5] else "")
                # if integer and primary key
                if farow[5] and farow[2].upper() == "INTEGER":
                    if "AUTOINCREMENT" in sql:  # Sequence is only created after first insert
                        dtype = AUTOINCREMENT
                existing_columns.append((farow[1], dtype))

            if not column_definitions:
                column_definitions = existing_columns
            else:
                column_definitions_dict = {name.upper(): dtype for name, dtype in column_definitions}
                for col, dtype in existing_columns:
                    if col.upper() not in column_definitions_dict:
                        raise Exception(f"Column '{col}' exists but is not in the provided columns.")
                    if column_definitions_dict[col.upper()] != dtype.upper():
                        raise Exception(f"Column '{col}' exists with a different type (database: {dtype} vs provided: {column_definitions_dict[col.upper()]}) in table {_name} in {db.db_name} .")
                existing_columns_dict = {col.upper(): dtype for col, dtype in existing_columns}
                for col, dtype in column_definitions:
                    if col.upper() not in existing_columns_dict:
                        self.db.execute(f"ALTER TABLE {self.name} ADD COLUMN {col} {dtype};")
        else:
            self.db.execute(f"CREATE {'TEMP' if self.temp else ''} TABLE {self.name} ({', '.join([f'{col} {dtype}' for col, dtype in column_definitions])})")
        self.column_definitions = column_definitions
        self.columns = [col for col, _ in column_definitions]
        self.query = f"SELECT {', '.join(self.columns)} FROM {self.name}"

    def __repr__(self):
        return f"Table({self.name}, {self.column_definitions})"

    def __str__(self):
        return f"Table {self.name}:\n" + "\n".join([f"{col}: {dtype}" for col, dtype in self.column_definitions])

    def insert(self, ignore=False, **values):
        self.db.insert(self.name, ignore=ignore, **self.back_from_bool(values))

    def delete(self, id=None, **values):
        if id is not None:
            values['id'] = id
        self.db.delete(self.name, **self.back_from_bool(values))

    def update(self, where: dict, **values):
        # print(f"Table: Updating {self.name} with {where} and {values}")
        self.db.update(self.name, self.back_from_bool(where), **self.back_from_bool(values))
    
    def call_update_cbs(self, table, old, new):
        if table != self.name:
            return
        old_values = [old[col] for col in self.columns]
        new_values = [new[col] for col in self.columns]
        old2 = {k: v for k, v in zip(self.columns, self.maybe_to_bool(old_values))}
        new2 = {k: v for k, v in zip(self.columns, self.maybe_to_bool(new_values))}
        for cb in self.update_cbs:
            cb(old2, new2)
    
    def call_insert_cbs(self, table, values):
        if table != self.name:
            return
        if DEBUG:   
            print("Table insert", values)
        values_array = [values[col] for col in self.columns]
        values2 = {k: v for k, v in zip(self.columns, self.maybe_to_bool(values_array))}
        for cb in self.insert_cbs:
            cb(values2)
    
    def call_delete_cbs(self, table, row):
        if table != self.name:
            return
        values = Row({k: v for k, v in zip(self.columns, self.maybe_to_bool(row))}, self.name)
        for cb in self.delete_cbs:
            cb(values)
    
    def __del__(self):
        self.db.update_cbs.remove(self.update_cbs_ref)
        self.db.insert_cbs.remove(self.insert_cbs_ref)
        self.db.delete_cbs.remove(self.delete_cbs_ref)

    def update_urlm(self, where: dict, **values):
        if where:
            raise NotImplementedError("Not empty update where URL not yet implemented")
        return URLM(f"/{self.name}?{urlencode({**values})}", "patch")
    
    def delete_urlm(self, **values):
        return URLM(f"/{self.name}?{urlencode(values)}", "delete")

class Value():
    def __init__(self, value):
        self.value = value
        self.cbs = []

    def map(self, f):
        return MapValue(self, f)
    
    def onchange(self, cb):
        self.cbs.append(cb)

    def onstr(self, cb):
        self.cbs.append(lambda v: cb(str(v)))

    def onvalue(self, cb):
        cb(self.value)
        self.onchange(cb)

    def __html__(self):
        r = self.tohtml(self)
        while hasattr(r, "__html__"):
            r = r.__html__()
        return r
    
    def tohtml(self, valueobj):
        try: 
            r = self.parent.tohtml(valueobj)
        except:
            raise Exception(f"error in tohtml: {self}, parent: {self.parent}, self.parent.tohtml: {self.parent.tohtml}, valueobj: {valueobj}, traceback: {traceback.format_exc()}")
        return r

class MapValue(Value):
    def __init__(self, parent, f):
        self.parent = parent
        self.f = f
        self.parent.onchange(self.call_cbs)
        super().__init__(self.f(self.parent.value))

    def call_cbs(self, v):
        self.value = self.f(v)
        for cb in self.cbs:
            cb(self.value)

    def __repr__(self):
        return f"MapValue({self.parent}, {self.f} {self.value})"
    
    def __str__(self):
        return f"MapValue({self.parent}, {self.f} {self.value})"

# weakmethod
from weakref import WeakMethod, ref
# how to use weakmethod?
def methodref(m):
    r = WeakMethod(m)
    def f(*args, **kwargs):
        rr = r()
        if rr is None:
            return
        return rr(*args, **kwargs)
    return f
class ColumnValue(Value):
    def __init__(self, parent, column):
        self.parent = parent
        self.column = column
        super().__init__(self.parent.fetchone()[self.column])
        self.wupdate = methodref(self.call_update_cbs)
        self.winsert = methodref(self.call_insert_cbs)
        self.wdelete = methodref(self.call_delete_cbs)
        self.wreset = methodref(self.call_reset_cbs)
        self.parent.update_cbs.append(self.wupdate)
        self.parent.insert_cbs.append(self.winsert)
        self.parent.delete_cbs.append(self.wdelete)
        self.parent.reset_cbs.append(self.wreset)
        

    def __repr__(self):
        return f"ColumnValue({self.parent}, {self.column} = {self.value})"
    
    def __str__(self):
        return f"ColumnValue({self.parent}, {self.column} = {self.value})"
    
    def call_update_cbs(self, _, new):
        self.value = new[self.column]
        for cb in self.cbs:
            cb(new[self.column])
    
    def call_insert_cbs(self, new):
        self.value = new[self.column]
        for cb in self.cbs:
            cb(new[self.column])
    
    def call_delete_cbs(self, _):
        self.value = None
        for cb in self.cbs:
            cb(None)
    
    def call_reset_cbs(self):
        old = self.value
        self.value = self.parent.fetchone()[self.column]
        for cb in self.update_cbs:
            cb(self.value)

    def __del__(self):
        self.parent.update_cbs.remove(self.wupdate)
        self.parent.insert_cbs.remove(self.winsert)
        self.parent.delete_cbs.remove(self.wdelete)
        self.parent.reset_cbs.remove(self.wreset)

class RowValue(Value):
    def __init__(self, parent):
        self.parent = parent
        super().__init__(self.parent.fetchone())
        self.update_cbs_ref = methodref(self.call_update_cbs)
        self.insert_cbs_ref = methodref(self.call_insert_cbs)
        self.delete_cbs_ref = methodref(self.call_delete_cbs)
        self.parent.update_cbs.append(self.update_cbs_ref)
        self.parent.insert_cbs.append(self.insert_cbs_ref)
        self.parent.delete_cbs.append(self.delete_cbs_ref)
    
    def __repr__(self):
        return f"RowValue({self.parent}, {self.value})"
    
    def __str__(self):
        return f"RowValue({self.parent}, {self.value})"
    
    def call_update_cbs(self, _, new):
        self.value = Row(new, self.parent)
        for cb in self.cbs:
            cb(self.value)
    
    def call_insert_cbs(self, new):
        self.value = Row(new, self.parent)
        for cb in self.cbs:
            cb(self.value)
    
    def call_delete_cbs(self, _):
        self.value = None
        for cb in self.cbs:
            cb(None)
    
    def __del__(self):
        self.parent.update_cbs.remove(self.update_cbs_ref)
        self.parent.insert_cbs.remove(self.insert_cbs_ref)
        self.parent.delete_cbs.remove(self.delete_cbs_ref)

class GroupBy(View):
    def __init__(self, parent, *group_by_columns, **aggregates):
        self.parent = parent
        super().__init__(parent.db)
        self.group_by_columns = group_by_columns
        self.aggregates = aggregates

        # Determine the new columns
        self.columns = list(group_by_columns)
        self.is_bool = list(map(lambda col: parent.is_bool[parent.columns.index(col)], group_by_columns)) if parent.is_bool else [False] * len(group_by_columns)
        # Check if there's a COUNT aggregation, if not, add it
        has_count = any(istartswith(func, 'COUNT') for func in aggregates.values())
        if not has_count:
            aggregates['_count'] = 'COUNT(*)'
        for alias, func in aggregates.items():
            self.columns.append(alias)
            self.is_bool.append(False)

        # Construct the new query
        group_by_clause = ", ".join(group_by_columns) if group_by_columns else ""
        aggregate_clause = ", ".join([f"{func} AS {alias}" for alias, func in aggregates.items()])
        self.query = f"SELECT {group_by_clause}{', ' if group_by_clause else ''}{aggregate_clause} FROM ({parent.query})"
        if group_by_columns:
            self.query += f" GROUP BY {group_by_clause}"

        result_set = self.db.execute(self.query)

        # Create a dictionary to map group_by values to their corresponding row in the result set
        self.group_map = {}
        for row in result_set:
            group_key = tuple(row[i] for i, col in enumerate(self.columns) if col in self.group_by_columns)
            self.group_map[group_key] = row

        self.count_col_name = next((alias for alias, func in aggregates.items() if istartswith(func, 'COUNT')), None)
        self.count_col_index = self.columns.index(self.count_col_name)

    def call_update_cbs(self, old, new):

        # Check if the group has changed
        old_group_values = tuple(old[col] for col in self.group_by_columns)
        new_group_values = tuple(new[col] for col in self.group_by_columns)

        if old_group_values != new_group_values:
            # If the group has changed, treat it as a delete from old group and insert into new group
            self.call_delete_cbs(old)
            self.call_insert_cbs(new)
        else:
            # If the group hasn't changed, update the aggregates
            old_group = self.group_map.get(old_group_values)
            new_group = list(old_group_values)

            for alias, func in self.aggregates.items():
                old_value = old_group[self.columns.index(alias)]
                if istartswith(func, 'COUNT'):
                    new_group.append(old_value)  # Count doesn't change for updates
                elif istartswith(func, 'SUM'):
                    new_group.append(old_value - old[func.split('(')[1][:-1]] + new[func.split('(')[1][:-1]])
                elif istartswith(func, 'AVG'):
                    count = old_group[self.count_col_index]
                    new_sum = old_value * count - old[func.split('(')[1][:-1]] + new[func.split('(')[1][:-1]]
                    new_group.append(new_sum / count)
                elif istartswith(func, 'MAX'):
                    new_value = new[func.split('(')[1][:-1]]
                    if new_value > old_value:
                        new_group.append(new_value)
                    elif new_value < old_value and old_value == old[func.split('(')[1][:-1]]:
                        # Need to query for the new max
                        column = func.split('(')[1][:-1]
                        where_clause, remaining_values = create_where_null_clause(self.group_by_columns, group_by_values)
                        query = f"SELECT MAX({column}) FROM ({self.parent.query}){where_clause}"
                        new_max = self.db.fetchone(query, remaining_values)[0]
                        new_group.append(new_max)
                    else:
                        new_group.append(old_value)
                elif istartswith(func, 'MIN'):
                    new_value = new[func.split('(')[1][:-1]]
                    if new_value < old_value:
                        new_group.append(new_value)
                    elif new_value > old_value and old_value == old[func.split('(')[1][:-1]]:
                        # Need to query for the new min
                        column = func.split('(')[1][:-1]
                        where_clause, remaining_values = create_where_null_clause(self.group_by_columns, group_by_values)
                        query = f"SELECT MIN({column}) FROM ({self.parent.query}){where_clause}"
                        new_min = self.db.fetchone(query, remaining_values)[0]
                        new_group.append(new_min)
                    else:
                        new_group.append(old_value)

            new_group = tuple(new_group)
            self.group_map[new_group_values] = new_group

            for cb in self.update_cbs:
                cb(dict(zip(self.columns, old_group)), dict(zip(self.columns, new_group)))

    def call_insert_cbs(self, values):
        # Store previous group map result
        group_by_values = tuple(values[col] for col in self.group_by_columns)
        prev_group = self.group_map.get(group_by_values)

        # Update the group map for aggregate functions
        old_group = self.group_map.get(group_by_values, None)
        new_group = list(group_by_values)
        for alias, func in self.aggregates.items():
            if old_group is None:
                # This is a new group
                if istartswith(func, 'COUNT'):
                    new_group.append(1)
                elif istartswith(func, 'SUM') or istartswith(func, 'AVG'):
                    new_group.append(values[func.split('(')[1][:-1]])
                elif istartswith(func, 'MAX') or istartswith(func, 'MIN'):
                    new_group.append(values[func.split('(')[1][:-1]])
            else:
                old_value = old_group[self.columns.index(alias)]
                if istartswith(func, 'COUNT'):
                    new_group.append(old_value + 1)
                elif istartswith(func, 'SUM'):
                    new_group.append(old_value + values[func.split('(')[1][:-1]])
                elif istartswith(func, 'AVG'):
                    count = old_group[self.count_col_index] + 1
                    new_sum = old_value * (count - 1) + values[func.split('(')[1][:-1]]
                    new_group.append(new_sum / count)
                elif istartswith(func, 'MAX'):
                    new_group.append(max(old_value, values[func.split('(')[1][:-1]]))
                elif istartswith(func, 'MIN'):
                    new_group.append(min(old_value, values[func.split('(')[1][:-1]]))
        new_group = tuple(new_group)

        if new_group:
            self.group_map[group_by_values] = new_group
        
        if not prev_group and not group_by_values:
            prev_group = tuple([0 if istartswith(func, 'COUNT') else None for func in self.aggregates.values()])

        # Determine if this is an insert or update
        if prev_group:
            for cb in self.update_cbs:
                cb(dict(zip(self.columns, prev_group)), dict(zip(self.columns, new_group)))
        else:
            for cb in self.insert_cbs:
                cb(dict(zip(self.columns, new_group)))

    def call_delete_cbs(self, values):
        # Store previous group map result
        group_by_values = tuple(values[col] for col in self.group_by_columns)
        prev_group = self.group_map.get(group_by_values)
        if prev_group is None:
            # The group doesn't exist, nothing to delete
            return
        
        # if count is 1 and group_by_values is empty, need to update to 0, None, None, 0...
        count = prev_group[self.count_col_index]
        if count == 1 and not group_by_values:
            new_group = tuple([0 if istartswith(func, 'COUNT') else None for func in self.aggregates.values()])
            del self.group_map[group_by_values]  # Still delete the only group
            for cb in self.update_cbs:
                cb(dict(zip(self.columns, prev_group)), dict(zip(self.columns, new_group)))
            return
        
        # Update the group map for aggregate functions
        new_group = list(group_by_values)
        for alias, func in self.aggregates.items():
            old_value = prev_group[self.columns.index(alias)]
            if istartswith(func, 'COUNT'):
                new_value = old_value - 1
                if new_value == 0:
                    # If count reaches 0 and group_by_values is not empty remove the group
                    del self.group_map[group_by_values] 
                    for cb in self.delete_cbs:
                        cb(dict(zip(self.columns, prev_group)))
                    return
                new_group.append(new_value)
            elif istartswith(func, 'SUM'):
                new_group.append(old_value - values[func.split('(')[1][:-1]])
            elif istartswith(func, 'AVG'):
                count = prev_group[self.count_col_index] - 1
                if count == 0:
                    # If count reaches 0, remove the group
                    del self.group_map[group_by_values]
                    for cb in self.delete_cbs:
                        cb(dict(zip(self.columns, prev_group)))
                    return
                new_sum = old_value * (count + 1) - values[func.split('(')[1][:-1]]
                new_group.append(new_sum / count)
            elif istartswith(func, 'MAX') or istartswith(func, 'MIN'):
                # For MAX and MIN, we need to recalculate only if the deleted value matches the current aggregate
                if (istartswith(func, 'MAX') and values[func.split('(')[1][:-1]] == old_value) or \
                   (istartswith(func, 'MIN') and values[func.split('(')[1][:-1]] == old_value):
                    where_clause, remaining_values = create_where_null_clause(self.group_by_columns, group_by_values)
                    query = f"SELECT {func} FROM ({self.parent.query}){where_clause}"
                    new_value = self.db.fetchone(query, remaining_values)[0]
                    new_group.append(new_value)
                else:
                    new_group.append(old_value)

        new_group = tuple(new_group)
        self.group_map[group_by_values] = new_group

        for cb in self.update_cbs:
            cb(dict(zip(self.columns, prev_group)), dict(zip(self.columns, new_group)))
    
    def fetchone(self, **values):
        if not values and tuple([]) in self.group_map:
            return Row(dict(zip(self.columns, self.group_map[tuple([])])), self)
        return super().fetchone(**values)

CHECK_SAME_THREAD = False
import gc

def hash(x):
    if isinstance(x, dict):
        return hash(frozenset(x.items()))
    return x.__hash__()


def track_view(obj):
    """
    Track a view and its associated rows.

    This function creates a list of rows from the view and adds callbacks
    to track changes to the view.

    Args:
        obj (View): The view object to track.

    Returns:
        tuple: A tuple containing the rows, a list of rows, and the insert and update callbacks.
    """
    rows = obj.fetchall()
    def delete_row(row):
        try:
            rowa = tuple([row[col] for col in obj.columns])
            rows.remove(rowa)
        except ValueError:
            raise Exception(f"Row not found: {rowa} in {rows} for {obj.query}")
        print(f"track_view delete_row from {obj}: {row} {rows}")
    insert_cb = lambda x: print(f"track_view insert_cb {x} {rows}") or rows.append(tuple([x[col] for col in obj.columns]))
    obj.insert_cbs.append(insert_cb)
    update_cb = lambda old, new: print(f"track_view update_cb {old} {new}") or delete_row(old) or insert_cb(new)
    obj.update_cbs.append(update_cb)
    obj.delete_cbs.append(delete_row)
    reset_cb = lambda: rows.clear() or rows.extend(obj.fetchall())
    obj.reset_cbs.append(reset_cb)
    return rows, [row for row in rows], insert_cb, update_cb, delete_row, reset_cb

def track_views():
    """
    Track views and their associated rows.

    This function iterates over all objects in the current Python environment,
    checks if they are instances of the View class, and if so, tracks them.
    It also tracks the rows, callbacks, and other related information for each view.

    Returns:    
        end_track: A function to end the tracking process and validate views.
    """
    views = []
    for obj in gc.get_objects():
        if isinstance(obj, View) and not isinstance(obj, Sort):
            views.append([obj, *track_view(obj)])
    def end_track(query):
        print("end_track", [[type(view), rows] for view, rows, before, insert_cb, update_cb, delete_cb, reset_cb in views])
        for view, rows, before, insert_cb, update_cb, delete_cb, reset_cb in views:
            view.insert_cbs.remove(insert_cb)
            view.update_cbs.remove(update_cb)
            view.delete_cbs.remove(delete_cb)
            view.reset_cbs.remove(reset_cb)
            rows2 = view.fetchall()
            rows2.sort(key=hash)
            rows.sort(key=hash)
            if rows != rows2:
                raise Exception(f"Views don't match for {type(view)} defined by {view.query}: {query}:\ncomputed: {rows}\nactual: {rows2}\nbefore: {before}")
    return end_track

class Database:
    """
    Represents a SQLite database with reactive capabilities.

    This class provides methods for creating tables, executing queries,
    and managing database operations with reactive updates.

    Attributes:
        conn (sqlite3.Connection): The database connection.
        update_cbs (list): Callbacks for update operations.
        insert_cbs (list): Callbacks for insert operations.
        delete_cbs (list): Callbacks for delete operations.

    Methods:
        execute(query, params=None): Execute a SQL query.
        create_table(name, **columns): Create a new table in the database.
        insert(table, ignore=False, **values): Insert a new row into a table.
        update(table, where, **values): Update rows in a table.
        delete(table, **where): Delete rows from a table.
        ...
    """

    def __init__(self, db_name: str, use_triggers=True):
        """
        Initialize a new Database instance.

        Args:
            db_name (str): The name of the SQLite database file.
            use_triggers (bool): Whether to use triggers for change tracking.
        """
        self.use_triggers = use_triggers
        self.db_name = db_name
        self.conn = sqlite3.connect(db_name, check_same_thread=CHECK_SAME_THREAD)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self.conn.execute("PRAGMA temp_store=MEMORY;")
        self.conn.execute("PRAGMA journal_size_limit=6144000;")
        self.conn.execute("PRAGMA mmap_size=134217728;") # 128MB
        self.local = threading.local()
        self.cursor = self.conn.cursor()
        self.lock = threading.RLock()
        self.tohtml = None

        self.tables = {}
        self.insert_cbs = []
        self.update_cbs = []
        self.delete_cbs = []
        self.get_cursor()

    def get_cursor(self):
        """
        Get the database cursor.

        Returns:
            sqlite3.Cursor: The database cursor.
        """
        return self.cursor

    def tables(self):
        """
        Get a list of all tables in the database.

        Returns:
            list: A list of table names.
        """
        return [row[0] for row in self.execute("SELECT name FROM sqlite_master WHERE type='table'")]

    def table(self, table_name: str, id=AUTOINCREMENT, temp=False, **columns: Union[str, Type]) -> Table:
        """
        Create or get a table in the database.

        Args:
            table_name (str): The name of the table.
            id: The primary key configuration.
            temp (bool): Whether the table is temporary.
            **columns: Column definitions.

        Returns:
            Table: The created or existing table object.
        """
        if table_name not in self.tables:
            table = Table(self, table_name, id=id, temp=temp, **columns)
            self.tables[table_name] = table
            if self.use_triggers:
                col_defs = [(col, "INTEGER" if "PRIMARY KEY" in dtype else dtype) for col, dtype in table.column_definitions]
                self.execute(
                    f"CREATE TEMP TABLE {table_name}_rows (action INTEGER, {', '.join([f'old_{col} {dtype}, new_{col} {dtype}' for col, dtype in col_defs])})")
                self.execute(
                    f"CREATE TEMP TRIGGER {table_name}_insert AFTER INSERT ON {table_name} BEGIN INSERT INTO {table_name}_rows (action, {', '.join([f'new_{col}' for col in table.columns])}) VALUES (1, {', '.join([f'NEW.{col}' for col in table.columns])}); END;")
                self.execute(
                    f"CREATE TEMP TRIGGER {table_name}_update AFTER UPDATE ON {table_name} BEGIN INSERT INTO {table_name}_rows (action, {', '.join([f'old_{col}, new_{col}' for col in table.columns])}) VALUES (2, {', '.join([f'OLD.{col}, NEW.{col}' for col in table.columns])}); END;")
                self.execute(
                    f"CREATE TEMP TRIGGER {table_name}_delete AFTER DELETE ON {table_name} BEGIN INSERT INTO {table_name}_rows (action, {', '.join([f'old_{col}' for col in table.columns])}) VALUES (3, {', '.join([f'OLD.{col}' for col in table.columns])}); END;")
        return self.tables[table_name]

    def respond_to_changes(self, table_name):
        """
        Respond to changes in the specified table or all tables.

        Args:
            table_name (str or None): The name of the table to respond to changes, or None for all tables.
        """
        if table_name == None:
            for table_name in self.tables.keys():
                self.respond_to_changes(table_name)
        else:
            actions = self.conn.execute(f"SELECT * FROM {table_name}_rows;").fetchall()
            for action in actions:
                if action[0] == 1:
                    for cb in self.insert_cbs:
                        cb(table_name, Row({k: v for k, v in zip(self.tables[table_name].columns, action[2::2])}, table_name))
                elif action[0] == 2:
                    for cb in self.update_cbs:
                        cb(table_name, Row({k: v for k, v in zip(self.tables[table_name].columns, action[1::2])}, table_name),
                           Row({k: v for k, v in zip(self.tables[table_name].columns, action[2::2])}, table_name))
                elif action[0] == 3:
                    for cb in self.delete_cbs:
                        cb(table_name, action[1::2])
            self.conn.execute(f"DELETE FROM {table_name}_rows;")
    
    def insert(self, table_name: str, ignore=False, **values):
        """
        Insert a new row into the specified table.

        Args:
            table_name (str): The name of the table.
            ignore (bool): Whether to ignore conflicts.
            **values: The values to insert.
        """
        with self.lock:
            cursor = self.get_cursor()
            try:
                if DEBUG_VIEWS:
                    views = track_views()
                query = f"INSERT {'OR IGNORE' if ignore else ''} INTO {table_name} ({', '.join(values.keys())}) VALUES ({', '.join(['?' for _ in values])})"
                execute(cursor, query, tuple(values.values()))
                if ignore and cursor.rowcount == 0:
                    return
                if self.use_triggers:
                    self.respond_to_changes(table_name)
                else:
                    cursor.execute(f"SELECT * FROM {table_name} WHERE rowid = ?", (cursor.lastrowid,))
                    inserted_row = cursor.fetchone()
                    data = {k[0]: v for k, v in zip(cursor.description, inserted_row)}
                    for cb in self.insert_cbs:
                        cb(table_name, data)
                self.conn.commit()
                if DEBUG_VIEWS:
                    views(query)
            except Exception as e:
                self.conn.rollback()
                raise e

    def delete(self, table_name: str, **values):
        """
        Delete rows from the specified table.

        Args:
            table_name (str): The name of the table.
            **values: The conditions for deletion.
        """
        with self.lock:
            if DEBUG_VIEWS:
                views = track_views()
            cursor = self.get_cursor()
            # get deleted rows
            where_clause, remaining_values = create_where_null_clause(values.keys(), values.values())
            execute(cursor, f"SELECT * FROM {table_name} {where_clause}", remaining_values)
            deleted_rows = cursor.fetchall()
            description = [x[0] for x in cursor.description]
            for row in deleted_rows:
                where_clause, remaining_values = create_where_null_clause(description, row)
                execute(cursor, f"DELETE FROM {table_name} {where_clause}", remaining_values)
                if self.use_triggers:
                    self.respond_to_changes(table_name)
                else:
                    for row in deleted_rows:
                        for cb in self.delete_cbs:
                            cb(table_name, row)
            self.conn.commit()
            if DEBUG_VIEWS:
                views(f"DELETE FROM {table_name} {where_clause}")

    def update(self, table_name: str, where: dict, **values):
        """
        Update rows in the specified table.

        Args:
            table_name (str): The name of the table.
            where (dict): The conditions for updating.
            **values: The new values to set.
        """
        with self.lock:
            if DEBUG_VIEWS:
                views = track_views()
            cursor = self.get_cursor()
            try:
                where_clause, remaining_values = create_where_null_clause(where.keys(), where.values())
                execute(cursor, f"SELECT * FROM {table_name} {where_clause};", remaining_values)
                old_rows = cursor.fetchall()
                description = [x[0] for x in cursor.description]
                set_clause = f"SET {', '.join([f'{k}=?' for k in values])}"
                execute(cursor, f"UPDATE {table_name} {set_clause} {where_clause};", tuple(values.values()) + remaining_values)
                if len(old_rows) != cursor.rowcount:
                    raise Exception("Update failed: where clause does not match any rows.")
                desc_from_upper = {k.upper(): k for k in description}
                updated_values = {desc_from_upper[k.upper()]: v for k, v in values.items()}
                for row in old_rows:
                    old = {k: v for k, v in zip(description, row)}
                    new = {**old, **updated_values}
                    if old == new:
                        continue
                    if self.use_triggers:
                        self.respond_to_changes(table_name)
                    else:
                        for cb in self.update_cbs:
                            cb(table_name, old, new)
                self.conn.commit()
                if DEBUG_VIEWS:
                    views(f"UPDATE {table_name} {set_clause} {where_clause}")
            except Exception as e:
                self.conn.rollback()
                raise e
    
    def execute(self, query, values=None):
        """
        Execute a SQL query on the database.

        Args:
            query (str): The SQL query to execute.
            values (tuple, optional): Parameters for the query.

        Returns:
            list: The result of the query execution.
        """
        with self.lock:
            if DEBUG:
                print("db.execute", query)
            cursor = self.get_cursor()
            try:
                if values:
                    cursor.execute(query, values)
                else:
                    cursor.execute(query)
                return cursor.fetchall()
            except Exception as e:
                self.conn.rollback()
                raise Exception(f"Error executing query: {query}" + (f" with values: {values}" if values else "")) from e

    def fetchone(self, query, values=None):
        """
        Execute a SQL query and fetch one result.

        Args:
            query (str): The SQL query to execute.
            values (tuple, optional): Parameters for the query.

        Returns:
            tuple: The first row of the query result.
        """
        with self.lock:
            if DEBUG:
                if values:
                    print(f"db.fetchone {query}, values: {values}")
                else:
                    print(f"db.fetchone {query}")
            cursor = self.get_cursor()
            try:
                if values:
                    cursor.execute(query, values)
                else:
                    cursor.execute(query)
                return cursor.fetchone()
            except Exception as e:
                self.conn.rollback()
                raise Exception(f"Error executing query: {query}" + (f" with values: {values}" if values else "")) from e
    
    def fetchall(self, query, values=None):
        """
        Execute a SQL query and fetch all results.

        Args:
            query (str): The SQL query to execute.
            values (tuple, optional): Parameters for the query.

        Returns:
            list: All rows of the query result.
        """
        with self.lock:
            if DEBUG:
                print("db.fetchall", query)
            cursor = self.get_cursor()
            try:
                if values:
                    cursor.execute(query, values)
                else:
                    cursor.execute(query)
                return cursor.fetchall()
            except Exception as e:
                self.conn.rollback()
                raise Exception(f"Error executing query: {query}" + (f" with values: {values}" if values else "")) from e

    def __del__(self):
        """
        Close the database connection when the object is deleted.
        """
        self.conn.close()

def sql_cmp(a, b):
    if a == b:
        return 0
    elif a is None:
        return -1
    elif b is None:
        return 1
    else:
        return (a > b) - (a < b)

class Sort(View):
    def __init__(self, parent, order_by=None, limit=None, offset=None):
        self.parent = parent
        super().__init__(parent.db)
        if isinstance(order_by, str):
            order_by = [order_by]
        self.order_by = order_by if order_by else []
        self.limit = limit
        self.offset = offset
        self.columns = parent.columns

        all_columns = parent.columns
        ordered_columns = set(col.split()[0] for col in self.order_by)  # order_by may contain DESC
        remaining_columns = [col for col in all_columns if col not in ordered_columns]
        self.order_by.extend(remaining_columns)
        self.reset()

    def reset_query(self):
        order_clause = f"ORDER BY {', '.join(self.order_by)}"
        limit_clause = f"LIMIT {self.limit}" if self.limit is not None else ""
        offset_clause = f"OFFSET {self.offset}" if self.offset is not None else ""
        self.query = f"SELECT * FROM ({self.parent.query}) {order_clause} {limit_clause} {offset_clause}"

    def reset(self):
        self.reset_query()
        self.sorted_results = self.fetchall()

    def set_offset(self, offset):
        if self.offset == offset:
            return
        self.offset = offset
        self.reset()
        for cb in self.reset_cbs:
            cb()    

    def set_order_by(self, order_by, limit=None):
        if isinstance(order_by, str):
            order_by = [order_by]
        if self.order_by == order_by:
            return
        self.order_by = order_by
        if limit:
            self.limit = limit
        self.reset()
        for cb in self.reset_cbs:
            cb()

    def set_limit(self, limit, reset=False):
        if self.limit == limit:
            return
        if reset:
            self.limit = limit
            self.reset()
            for cb in self.reset_cbs:
                cb()
        else:
            if self.limit is not None and self.limit > limit:
                self.limit = limit
                self.reset_query()
                for i in range(len(self.sorted_results) - 1, limit - 1, -1):
                    row = self.sorted_results.pop(i)
                    for cb in self.delete_cbs:
                        cb(i, Row({k: v for k, v in zip(self.parent.columns, row)}, self))
            else:
                old_limit = self.limit
                # Fetch new results
                order_clause = f"ORDER BY {', '.join(self.order_by)}"
                limit_clause = f"LIMIT {limit - self.limit}" if self.limit is not None else ""
                offset = self.offset if self.offset is not None else 0
                offset_clause = f"OFFSET {offset + self.limit}"
                query = f"SELECT * FROM ({self.parent.query}) {order_clause} {limit_clause} {offset_clause}"
                new_results = self.db.execute(query)
                self.sorted_results.extend(new_results)
                self.limit = limit
                limit_clause = f"LIMIT {self.limit}" if self.limit is not None else ""
                offset_clause = f"OFFSET {self.offset}" if self.offset is not None else ""
                self.query = f"SELECT * FROM ({self.parent.query}) {order_clause} {limit_clause} {offset_clause}"
                for i in range(old_limit, len(self.sorted_results)):
                    for cb in self.insert_cbs:
                        cb(i, dict(zip(self.parent.columns, self.sorted_results[i])))

    def call_insert_cbs(self, values):
        new_row = tuple(values[col] for col in self.parent.columns)
        insert_index = self.find_insert_index(new_row)
        if self.limit is not None and insert_index >= self.limit:
            return
        if self.offset is not None and self.offset > 0 and insert_index == 0:
            new_row = self.db.fetchone(self.query)
        self.sorted_results.insert(insert_index, new_row)
        
        if self.limit is not None and len(self.sorted_results) > self.limit:
            if insert_index < self.limit:
                removed_row = self.sorted_results.pop()
                for cb in self.delete_cbs:
                    cb(self.limit, dict(zip(self.parent.columns, removed_row)))

        for cb in self.insert_cbs:
            cb(insert_index, values)

    def call_update_cbs(self, old, new):
        old_row = tuple(old[col] for col in self.parent.columns)
        new_row = tuple(new[col] for col in self.parent.columns)
        if old_row not in self.sorted_results:
            self.call_delete_cbs(old)
            self.call_insert_cbs(new)
            return

        old_index = self.sorted_results.index(old_row)
        self.sorted_results.pop(old_index)
        new_index = self.find_insert_index(new_row)
        self.sorted_results.insert(new_index, new_row)

        for cb in self.update_cbs:
            cb(old_index, new_index, old, new)

    def less(self, row1, row2):
        return self.compare_rows(row1, row2) < 0

    def call_delete_cbs(self, values):
        row = tuple(values[col] for col in self.parent.columns)
        if row not in self.sorted_results:
            # Either too small or too big, need a select to find it out
            if self.less(self.sorted_results[-1], row):
                return
            index = 0
        else:
            index = self.sorted_results.index(row)
        if index == None:
            # If too big
            return
        self.sorted_results.pop(index)

        for cb in self.delete_cbs:
            cb(index, values)

        if self.limit is not None and len(self.sorted_results) == self.limit - 1:
            sorted_results = self.fetchall()
            if len(sorted_results) == self.limit:
                new_last_row = sorted_results[-1]
                self.sorted_results.append(new_last_row)
                for cb in self.insert_cbs:
                    cb(self.limit - 1, dict(zip(self.parent.columns, new_last_row)))

    def on_delete(self, cb):
        f = lambda index, row: print("calling zip for row", row, type(row), ", parent type: ", type(self.parent)) or cb(index, Row(row, self))
        self.delete_cbs.append(f)
        return lambda: self.delete_cbs.remove(f)

    def on_insert(self, cb):
        f = lambda index, row: cb(index, Row(row, self))
        self.insert_cbs.append(f)
        return lambda: self.insert_cbs.remove(f)

    def on_update(self, cb):
        f = lambda iold, inew, old, new: cb(iold, inew, Row(old, self), Row(new, self))
        self.update_cbs.append(f)
        return lambda: self.update_cbs.remove(f)
    
    def on_reset(self, cb):
        f = lambda: cb()
        self.reset_cbs.append(f)
        return lambda: self.reset_cbs.remove(f)

    def find_insert_index(self, row):
        return next((i for i, r in enumerate(self.sorted_results) if self.compare_rows(row, r) < 0), len(self.sorted_results))

    def compare_rows(self, row1, row2):
        # It's better to use the sql database engine for this
        for col in self.order_by:
            col_name = col.split()[0]
            col_index = self.parent.columns.index(col_name)
            if 'DESC' in col.upper():
                if sql_cmp(row1[col_index], row2[col_index]) > 0:
                    return -1
                elif sql_cmp(row1[col_index], row2[col_index]) < 0:
                    return 1
            else:
                if sql_cmp(row1[col_index], row2[col_index]) < 0:
                    return -1
                elif sql_cmp(row1[col_index], row2[col_index]) > 0:
                    return 1
        return 0

    def update(self, where, **values):
        self.parent.update(where, **values)
    
    def __iter__(self):
        return map(
            lambda values: Row(
                {col: val for col, val in zip(self.columns, self.maybe_to_bool(values))},
                self
            ),
            self.sorted_results
        )