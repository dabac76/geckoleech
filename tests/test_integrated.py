import json
from unittest.mock import patch, PropertyMock, Mock

import duckdb
import numpy as np
import pytest
from numpy.testing import assert_allclose
# WHEN QUERYING JsonQ OBJECT IN CONSOLE START WITH JsonQ.reset()!
from geckoleech.main import APIReq, leech, JsonQ, DuckDB


# Data contains extreme numerical ranges
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
    "GCR1: Return data, no calling arguments (params), multiple json/sql queries",
    fixed_req,
    None,
    [lambda rsp, ignore: list((rsp.at('.market_data.current_price').get()).items()),
     lambda rsp, ignore: [tuple(el[1] for el in list(rsp.at('.community_data').get().items())[1:3])]
     ],
    ["INSERT INTO main.tst VALUES (?, ?)",
     "INSERT INTO main.tst_soc VALUES (?, ?)"
     ]
)

gcr2 = APIReq(
    "GCR2: Raise API request exception to be logged",
    broken_req,
    None,
    [lambda x, ignore: x],  # Never called
    [" .. "]  # Never executed
)

# noinspection SqlResolve
gcr3 = APIReq(
    "GCR4: Return data, with calling arguments and generator json-query",
    fixed_req_args,
    ([{"USD", "EUR"}, {4, 8}], {}),
    [lambda rsp, ignore: (row for row in (rsp.get()).items())],
    ["INSERT INTO main.tst01 values (?, ?)"]
)


# noinspection SqlResolve
@patch.object(DuckDB, "DB_PATH", new_callable=PropertyMock)
@patch("geckoleech.main.logging")
@patch("geckoleech.main.sleep")
def test_integrated(sl: Mock, lg: Mock, db: Mock, ddb_prepare, capsys):

    db.return_value = "test.db"
    sl.side_effect = None

    leech()

    captured = capsys.readouterr()
    con = duckdb.connect("test.db", read_only=True)

    # GCR1 testcase assertions
    con.execute("SELECT * FROM main.tst;")
    actual = con.fetchnumpy()
    dt = np.dtype("U3, float")
    actual = np.sort(actual["price"])
    expected_gcr1 = gcr1.json_queries[0](JsonQ(data=fixed_req()), [])
    expected_gcr1 = np.array(expected_gcr1, dt)
    expected_gcr1["f1"].sort()
    expected_gcr1 = expected_gcr1["f1"]
    # With absolute tol <1e-05 round-off diff between db and python appear
    assert_allclose(actual, expected_gcr1, atol=.0001)
    assert f"SUCCESS: {gcr1.name} ->" in captured.out

    con.execute("SELECT * FROM main.tst_soc;")
    actual = [el for el in con.fetchall()[0]]
    expected = [4499460, 7.2]
    assert expected == pytest.approx(actual)

    # GCR2 testcase assertions
    lg.error.assert_called_once_with("REQUEST: %s -> %s | %s", gcr2.name, str(None), "BadRequest")
    assert f"NO_RESPONSE_DETAILS_LOGGED: {gcr2.name} ->" in captured.out

    # GCR4 testcase assertions
    con.execute("SELECT curr, CAST(SUM(price) AS DOUBLE) "
                "FROM main.tst01 GROUP BY curr;")
    actual = con.fetchall()
    expected = [("USD", 12.0), ("USDT", 24.0), ("EUR", 12.0), ("EURT", 24.0)]
    assert all([el in expected for el in actual])
    assert f"SUCCESS: {gcr3.name} ->" in captured.out

    # For the case when running all the tests
    APIReq._leeches = []
