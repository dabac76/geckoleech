import json
from unittest.mock import patch
import numpy as np
from numpy.testing import assert_allclose
from geckoleech.main import APIReq, leech, JsonQ


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

gcr3 = APIReq(
    "GCR3: Raise thread exception to be printed to stdout",
    fixed_req,
    None,
    lambda rsp: list(rsp.at('bad.json.key').get().items()),
    " .. "  # Never executed
)

# noinspection SqlResolve
gcr4 = APIReq(
    "GCR4: Return data, with calling arguments and generator query",
    fixed_req_args,
    ([{"USD", "EUR"}, {4, 8}], {}),
    lambda jsq: (row for row in (jsq.get()).items()),
    "INSERT INTO main.tst01 values (?, ?)"
)


# noinspection SqlResolve
@patch("geckoleech.main.logging")
@patch("geckoleech.main.sleep")
def test_integrated(sl, lg, ddb_cursor, capsys):

    sl.side_effect = None
    mock_log = []
    lg.error.side_effect = lambda msg1, msg2, msg3: mock_log.append(msg1+msg2+msg3)

    with patch("geckoleech.utils.DuckDB") as MockDB:
        instance = MockDB.return_value
        instance.__enter__.return_value = ddb_cursor
        leech()
    captured = capsys.readouterr()

    # GCR1 testcase assertions
    ddb_cursor.execute("SELECT * FROM main.tst;")
    actual = ddb_cursor.fetchall()
    dt = np.dtype("U3, float")
    actual = np.sort(np.array(actual, dt))
    expected_gcr1 = gcr1.json_query(JsonQ(data=fixed_req()))
    expected_gcr1 = np.sort(np.array(expected_gcr1, dt))
    # With absolute tol <1e-05 round-off diff between db and python appear
    # assert_allclose(actual["f1"], expected_gcr1["f1"], atol=.0001)
    assert "THREAD SUCCESS: GCR1" in captured.out

    # GCR2 testcase assertions
    lg.error.assert_called_once()
    assert any(["BadRequest" in line for line in mock_log])
    assert "THREAD SUCCESS: GCR2" in captured.out

    # GCR3 testcase assertions
    assert "THREAD FAILED: GCR3" in captured.out

    # GCR4 testcase assertions
    assert "THREAD SUCCESS: GCR4" in captured.out
    ddb_cursor.execute("SELECT curr, CAST(SUM(price) AS DOUBLE) "
                       "FROM main.tst01 GROUP BY curr;")
    actual = ddb_cursor.fetchall()
    expected = [("USD", 12.0), ("USDT", 24.0), ("EUR", 12.0), ("EURT", 24.0)]
    assert all([el in expected for el in actual])
