import pytest
import duckdb


# noinspection SqlWithoutWhere,SqlResolve
@pytest.fixture()
def ddb_prepare():
    con = duckdb.connect("test.db", read_only=False)
    con.execute("CREATE TABLE IF NOT EXISTS main.tst (curr VARCHAR, price DECIMAL(18,4));")
    con.execute("DELETE FROM main.tst;")
    # con.execute("CREATE TABLE IF NOT EXISTS main.tst01 (curr VARCHAR, price DECIMAL(18,4));")
    # con.execute("DELETE FROM main.tst01;")
    con.close()


# After DuckDB 0.3.2. update, read_only=False
# connection/cursor made in one thread cannot be used in another
# Nice trick to use the same conn inside tests, while it worked:
# (When patching a class, return value has to be set on instance
#  first)
# with patch("DuckDB") as MockDB:
#     instance = MockDB.return_value
#     instance.__enter__.return_value = ddb_cursor
#     main.leech()
# @pytest.fixture()
# def ddb_cursor(ddb_prepare):
#     con = duckdb.connect("test.db", read_only=True)
#     yield con
#     con.close()
