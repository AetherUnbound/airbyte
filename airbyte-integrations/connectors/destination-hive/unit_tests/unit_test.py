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

from unittest import mock

import pytest

from destination_hive.client import HiveClient


@pytest.fixture
def cursor_mock() -> mock.MagicMock:
    with mock.patch("impala.dbapi.connect") as connect:
        cursor = connect.return_value.cursor.return_value
        yield cursor


@pytest.fixture
def client(cursor_mock) -> HiveClient:
    hive = HiveClient("myhost", "myuser", "mypass", "mystream")
    yield hive


def test_get_table():
    assert HiveClient.get_table("mysamplestream") == "airbyte_json_mysamplestream"


def test_create_table(cursor_mock, client):
    client.create_table()
    sql = """
CREATE TABLE IF NOT EXISTS airbyte_json_mystream (
    json_data string
);
"""
    cursor_mock.execute.assert_called_with(sql)


def test_drop_table(cursor_mock, client):
    client.drop_table()
    sql = f"DROP TABLE {client.table} PURGE;"
    cursor_mock.execute.assert_called_with(sql)


@pytest.mark.parametrize(
    "overwrite, expected_sql",
    [
        (
            False,
            """
LOAD DATA LOCAL INPATH '/test/path'

INTO TABLE airbyte_json_mystream;
""",
        ),
        (
            True,
            """
LOAD DATA LOCAL INPATH '/test/path'
OVERWRITE
INTO TABLE airbyte_json_mystream;
""",
        ),
    ],
)
def test_load_data(cursor_mock, client, overwrite, expected_sql):
    client.load_data("/test/path", overwrite)
    cursor_mock.execute.assert_called_with(expected_sql)
