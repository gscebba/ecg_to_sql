#  Copyright (c) 2021. Gaetano Scebba
#  Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
#  documentation files (the "Software"), to deal in the Software without restriction, including without limitation
#  the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
#  and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in all copies or substantial portions
#   of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED
#  TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
#  THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF
#  CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
#  DEALINGS IN THE SOFTWARE.

from __future__ import print_function

import sqlite3
import sys
import traceback
from pathlib import Path

import pandas as pd


class DbAccess(object):
    def __init__(self, data_dir: Path, db_name: str):
        """

        :param data_dir: Path to the .db file
        :param db_name:  database file name
        """
        self.db = self.connect(data_dir, db_name)

    def connect(self, data_dir: Path, db_name: str):
        db = sqlite3.connect(str(data_dir / db_name))
        db.execute("PRAGMA journal_mode = OFF;")
        db.execute("PRAGMA page_size = 16384;")
        return db

    def read(self, query: str):
        query = self._append_semicolumn(query)
        cur = self.db.cursor()
        data = cur.execute(query).fetchall()
        cur.close()
        return data

    def write(self, query: str):
        query = self._append_semicolumn(query)
        cur = self.db.cursor()
        try:
            cur.execute(query)
            self.db.commit()
        except sqlite3.Error as er:
            print(f'SQLite error: {er.args}')
            print(f'Exception class: {er.__class__}')
            print('SQLite traceback: ')
            exc_type, exc_value, exc_tb = sys.exc_info()
            print(traceback.format_exception(exc_type, exc_value, exc_tb))
            print(query)
            self.db.close()
        cur.close()

    def write_many(self, query: str, values: list):
        query = self._append_semicolumn(query)
        cur = self.db.cursor()
        try:
            cur.executemany(query, values)
            self.db.commit()
        except sqlite3.Error as er:
            print(f'SQLite error: {er.args}')
            print(f'Exception class: {er.__class__}')
            print('SQLite traceback: ')
            exc_type, exc_value, exc_tb = sys.exc_info()
            print(traceback.format_exception(exc_type, exc_value, exc_tb))
            self.db.close()
        cur.close()

    def _check_structure(self):
        tables = pd.read_sql_query('SELECT * FROM sqlite_master ;', self.db)
        print(tables)

    def _check_table(self, table_name: str):
        table = pd.read_sql_query(f'SELECT * FROM {table_name};', self.db)
        print(table)

    @staticmethod
    def _append_semicolumn(query):
        if query[-1] != ';':
            query += ';'
        return query
