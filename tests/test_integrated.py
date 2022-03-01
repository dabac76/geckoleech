import json
from unittest.mock import patch, PropertyMock

import duckdb
import numpy as np
from numpy.testing import assert_allclose
from geckoleech.main import APIReq, leech, JsonQ, DuckDB


# "yfi": 1.5756362568548483
# "ars": 75747871453154.25
# "lkr": 145929530894430.2,
# "vnd": 16372077068486000,
def fixed_req():
    with open("./data.json", "r") as f:
        return json.load(f)


def broken_req():
    raise ValueError("BadRequest")


def fixed_req_args(curr, price, **kwargs):
    _ = kwargs.items()
    return {curr: price, curr+"T": price * 2}


# noinspection SqlResolve
gcr1 = APIReq(
    "GCR1: Return data, without calling arguments",
    fixed_req,
    None,
    lambda rsp: list((rsp.at('.market_data.current_price').get()).items()),
    "INSERT INTO main.tst values (?, ?)"
)

gcr2 = APIReq(
    "GCR2: Raise API request exception to be logged",
    broken_req,
    None,
    lambda x: x,  # Never called
    " .. "  # Never executed
)

# gcr3 = APIReq(
#     "GCR3: Raise thread exception to be printed to stdout",
#     fixed_req,
#     None,
#     lambda rsp: list(rsp.at('bad.json.key').get().items()),
#     " .. "  # Never executed
# )

# noinspection SqlResolve
gcr4 = APIReq(
    "GCR4: Return data, with calling arguments and generator query",
    fixed_req_args,
    ([{"USD", "EUR"}, {4, 8}], {}),
    lambda jsq: (row for row in (jsq.get()).items()),
    "INSERT INTO main.tst01 values (?, ?)"
)


# noinspection SqlResolve
@patch.object(DuckDB, "DB_PATH", new_callable=PropertyMock)
@patch("geckoleech.main.logging")
@patch("geckoleech.main.sleep")
def test_integrated(sl, lg, db, ddb_prepare, capsys):

    db.return_value = "test.db"
    sl.side_effect = None
    mock_log = []
    lg.error.side_effect = lambda msg1, msg2, msg3: mock_log.append(msg1+msg2+msg3)

    leech()

    captured = capsys.readouterr()
    con = duckdb.connect("test.db", read_only=True)

    # GCR1 testcase assertions
    con.execute("SELECT * FROM main.tst;")
    actual = con.fetchnumpy()
    dt = np.dtype("U3, float")
    actual = np.sort(actual["price"])
    expected_gcr1 = gcr1.json_query(JsonQ(data=fixed_req()))
    expected_gcr1 = np.array(expected_gcr1, dt)
    expected_gcr1["f1"].sort()
    expected_gcr1 = expected_gcr1["f1"]
    # With absolute tol <1e-05 round-off diff between db and python appear
    assert_allclose(actual, expected_gcr1, atol=.0001)
    assert "SUCCESS:" in captured.out

    # GCR2 testcase assertions
    lg.error.assert_called_once()
    assert any(["BadRequest" in line for line in mock_log])

    # GCR4 testcase assertions
    con.execute("SELECT curr, CAST(SUM(price) AS DOUBLE) "
                "FROM main.tst01 GROUP BY curr;")
    actual = con.fetchall()
    expected = [("USD", 12.0), ("USDT", 24.0), ("EUR", 12.0), ("EURT", 24.0)]
    assert all([el in expected for el in actual])
