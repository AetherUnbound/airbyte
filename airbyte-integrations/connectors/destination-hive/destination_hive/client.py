#
# MIT License
#
# Copyright (c) 2020 Airbyte
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#

import json
from typing import List, Optional, Dict

import impala.dbapi
from impala.interface import Cursor, Connection


class HiveClient:
    def __init__(
        self,
        host: str,
        username: str,
        password: str,
        database: str = "default",
        port: int = 10000,
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self._conn: Optional[Connection] = None
        self._cursor: Optional[Cursor] = None

    def __enter__(self):
        return self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def get_conn(self) -> Connection:
        conn = impala.dbapi.connect(
            host=self.host,
            port=self.port,
            user=self.username,
            password=self.password,
            database=self.database,
        )
        return conn

    def _open(self):
        self._conn = self.get_conn()
        self._cursor = self._conn.cursor()

    @property
    def cursor(self):
        if self._conn is None:
            self._open()
        return self._cursor

    def close(self):
        if self._cursor:
            self._cursor.close()
        if self._conn:
            self._conn.close()

    def open(self):
        self.close()
        self._open()
        return self

    def get_table(self, stream: str) -> str:
        return f"airbyte_json_{stream}"

    def write(self, record: Dict, stream: str) -> None:
        ...


    def read_data(self) -> List[Dict]:
        ...

    def delete(self) -> None:
        ...
