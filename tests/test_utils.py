from unittest.mock import patch
from datetime import datetime, timedelta
import pytest
import geckoleech.utils as utils


today = datetime.today().strftime("%d-%m-%Y")
yesterday = (datetime.today() - timedelta(days=1)).strftime("%d-%m-%Y")


@pytest.mark.parametrize("start, end", [(yesterday, None), ("01-01-2022", "03-01-2022")])
def test_dates_cg_format(start, end):
    if end is None:
        assert utils.dates_cg_format(start) == {yesterday, today}
    else:
        assert utils.dates_cg_format(start, end) == {"01-01-2022", "02-01-2022", "03-01-2022"}


@pytest.fixture()
def prep_expand():
    args = [{"date1", "date2"}, {1, 2}]
    kwargs = {"local": {True, False}, "page": 10}
    expected = [
        (("date1", 1), {"local": True, "page": 10}),
        (("date1", 2), {"local": True, "page": 10}),
        (("date2", 1), {"local": True, "page": 10}),
        (("date2", 2), {"local": True, "page": 10}),
        (("date1", 1), {"local": False, "page": 10}),
        (("date1", 2), {"local": False, "page": 10}),
        (("date2", 1), {"local": False, "page": 10}),
        (("date2", 2), {"local": False, "page": 10}),
    ]
    return (args, kwargs), expected


def test_expand(prep_expand):
    args_kwargs, expected = prep_expand
    # noinspection SpellCheckingInspection
    actuals = utils.expand(args_kwargs)
    assert len(actuals) == len(expected)
    assert all([actual in expected for actual in actuals])


@pytest.mark.parametrize("args_kwargs, expected", [
    (([1, 2, 3],), [(1, 2, 3)]),
    (([{0, 1}],), [(0,), (1,)]),
    (([{0, 1}, {2, 3}], {}), [(0, 2), (0, 3), (1, 2), (1, 3)]),
    (([{0, 1}], {"id": 1, "p": 1}), [((0,), {"id": 1, "p": 1}), ((1,), {"id": 1, "p": 1})])
])
def test_expand_edges(args_kwargs, expected):
    # noinspection SpellCheckingInspection
    actuals = utils.expand(args_kwargs)
    assert len(actuals) == len(expected)
    assert all([actual in expected for actual in actuals])


# noinspection SqlResolve
def test_ddb(ddb_cursor):
    rows = [("btc", 2), ("eth", 4)]
    row_gen = (row for row in rows)
    ddb_cursor.executemany("INSERT INTO main.tst VALUES (?, ?);", row_gen)
    ddb_cursor.execute("SELECT * FROM main.tst;")
    actual = ddb_cursor.fetchall()
    assert actual == rows


# noinspection SqlResolve
def test_save2db(ddb_cursor):
    rows = [("shitcoin0001", 2), ("shitcoin0002", 4)]
    row_gen = (row for row in rows)
    with patch("geckoleech.utils.DuckDB") as MockDB:
        instance = MockDB.return_value
        instance.__enter__.return_value = ddb_cursor
        utils.save2db("INSERT INTO main.tst VALUES (?, ?);", row_gen)
    ddb_cursor.execute("SELECT * FROM main.tst;")
    actual = ddb_cursor.fetchall()
    assert actual == rows
