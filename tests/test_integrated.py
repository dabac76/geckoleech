from unittest.mock import patch
from pyjsonq import JsonQ

import main
from geckoleech.main import APIReq


def fixed_req():
    return JsonQ("./data.json")


def broken_req():
    raise ValueError("BadRequest")


def fixed_req_args(curr, price):
    return [[curr, price], [curr, price*2]]


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
    "GRC3: Raise thread exception to be printed to stdout",
    fixed_req,
    None,
    lambda rsp: list(rsp.at('bad.json.key').get().items()),
    " .. "  # Never executed
)

# noinspection SqlResolve
gcr4 = APIReq(
    "GCR4: Return data, with calling arguments",
    fixed_req_args,
    ([{"usd", "eur"}, {4, 8}], {}),
    lambda x: iter(x),
    "INSERT INTO main.tst values (?, ?)"
)


# noinspection SqlResolve
@patch("geckoleech.main.logging")
@patch("geckoleech.main.sleep")
def test_integrated(sl, lg, ddb_cursor, capsys):
    sl.side_effect = None
    mock_log = []
    lg.error.side_effect = lambda msg: mock_log.append(msg)
    with patch("geckoleech.utils.DuckDB") as MockDB:
        instance = MockDB.return_value
        instance.__enter__.return_value = ddb_cursor
        main.leech()
    ddb_cursor.execute("SELECT * FROM main.tst;")
    actual = ddb_cursor.fetchall()
    expected_gcr1 = gcr1.json_query(fixed_req())
    expected_gcr4 = [("usd", 4), ("usd", 8), ("eur", 4), ("eur", 8)]
    captured = capsys.readouterr()
    lg.error.assert_called_once()
    assert "BadRequest" in mock_log
    assert all([expect in actual for expect in expected_gcr1])
    assert all([expect in actual for expect in expected_gcr4])
    assert "THREAD FAILED:" in captured.out
