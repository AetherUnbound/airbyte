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
import contextlib
from typing import List, Optional, Tuple

import impala.dbapi
from impala.interface import Cursor, Connection


class HiveClient:
    def __init__(
        self,
        host: str,
        username: str,
        password: str,
        stream: str,
        database: str = "default",
        port: int = 10000,
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.stream = stream
        self.table = self.get_table(stream)

    def get_conn(self) -> Connection:
        conn = impala.dbapi.connect(
            host=self.host,
            port=self.port,
            user=self.username,
            password=self.password,
            database=self.database,
            auth_mechanism="PLAIN",
        )
        return conn

    @staticmethod
    def get_table(stream: str) -> str:
        return f"airbyte_json_{stream}"

    def execute(self, sql: str, fetch: bool = False) -> Optional[List[Tuple]]:
        conn: Connection
        cursor: Cursor
        with contextlib.closing(self.get_conn()) as conn:
            with contextlib.closing(conn.cursor()) as cursor:
                cursor.execute(sql)
                if fetch:
                    return cursor.fetchall()

    def create_table(self) -> None:
        self.execute(
            f"""
CREATE TABLE IF NOT EXISTS {self.table} (
    json_data string
)
"""
        )

    def drop_table(self) -> None:
        self.execute(f"DROP TABLE {self.table} PURGE")

    def load_data(self, path: str, overwrite: bool = False) -> None:
        self.create_table()
        self.execute(
            f"""
LOAD DATA LOCAL INPATH '{path}'
{"OVERWRITE" if overwrite else ""}
INTO TABLE {self.table}
"""
        )

    def read_data(self) -> List[Tuple]:
        data = self.execute(f"SELECT * FROM {self.table} LIMIT 1000", fetch=True)
        if not data:
            raise ValueError(f"Query on {self.table} returned no results!")
        return data
