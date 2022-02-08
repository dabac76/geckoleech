import pytest
import duckdb


# noinspection SqlWithoutWhere,SqlResolve
@pytest.fixture()
def ddb_cursor():
    con = duckdb.connect("test.db", read_only=False)
    con.execute("CREATE TABLE IF NOT EXISTS main.tst (curr VARCHAR, price DECIMAL(18,4));")
    con.execute("DELETE FROM main.tst;")
    con.execute("CREATE TABLE IF NOT EXISTS main.tst01 (curr VARCHAR, price DECIMAL(18,4));")
    con.execute("DELETE FROM main.tst01;")
    yield con.cursor()
    con.close()
