import pytest
import duckdb


# noinspection SqlWithoutWhere,SqlResolve
@pytest.fixture()
def ddb_cursor():
    con = duckdb.connect("test.db", read_only=False)
    con.execute("CREATE TABLE IF NOT EXISTS main.tst (curr VARCHAR, price FLOAT);")
    con.execute("DELETE FROM main.tst;")
    yield con.cursor()
    con.close()
