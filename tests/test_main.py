import threading
from collections import namedtuple
from queue import Queue, Empty
from unittest.mock import Mock, patch

import duckdb

import geckoleech.main as main


def test_class_counter():
    gco1 = main.APIReq("gc01", Mock(), Mock(), Mock(), Mock())
    gco2 = main.APIReq("gc02", Mock(), Mock(), Mock(), Mock())
    assert main.APIReq.all() == [gco1, gco2]


@patch("geckoleech.main.logging")
@patch("geckoleech.main.sleep")
@patch("geckoleech.main.expand")
@patch("geckoleech.main.APIReq")
def test_task_log(req, xp, sl, lg):
    sl.side_effect = None
    xp.return_value = [("once",)]
    req.req.side_effect = Exception("ConnError")
    args = ("REQUEST Error: %s | %s", str(("once",)), "ConnError")
    main._task(req, Queue())
    lg.error.assert_called_once_with(*args)


# noinspection SqlResolve
@patch("geckoleech.main.logging")
@patch("geckoleech.main._task")
@patch.object(main.DuckDB, "DB_PATH")
@patch.object(main.APIReq, "all")
def test_ddb_thread(all_resp, db, task, lg, ddb_cursor):

    db.return_value = "./tests/test.db"
    # lg.error.reset_mock()

    APIResp = namedtuple("APIResp", "data name")
    # all_resp.return_value = \
    #     [APIResp([("shitcoin0001", 2), ("shitcoin0002", 4)], "TASK1"),
    #      APIResp([("shitcoin0003", 8), ("shitcoin0004", 16)], "TASK2"),
    #      APIResp([ValueError("Row Generator Error")], "TASK3")]
    all_resp.return_value = \
        [APIResp([("shitcoin0001", 2), ("shitcoin0002", 4)], "TASK1"),
         APIResp([("shitcoin0003", 8), ("shitcoin0004", 16)], "TASK2")]

    stmt = "INSERT INTO main.tst VALUES (?, ?);"
    # task.side_effect = lambda r, q: q.put((stmt, r.data))

    # main.leech()

    q = Queue()
    # evt = threading.Event()
    q.put((stmt, [("shitcoin0001", 2), ("shitcoin0002", 4)]))
    q.put((stmt, [("shitcoin0003", 8), ("shitcoin0004", 16)]))

    def _th1(qq: Queue):
        con = duckdb.connect("test.db", read_only=False)
        while qq.not_empty:
            try:
                data = qq.get()
            except Empty:
                continue
            else:
                print("\n")
                print(data)
                con.executemany(*data)

    dbt = threading.Thread(target=_th1, args=(q,), daemon=True)
    dbt.start()

    ddb_cursor.execute("SELECT * FROM main.tst;")
    actuals = ddb_cursor.fetchall()
    expected = [*all_resp.return_value[0].data, *all_resp.return_value[1].data]
    print("\n")
    print(actuals)
    assert all([actual in expected for actual in actuals])

    # args = ("DUCKDB Error: %s | %s", stmt, "Row Generator Error")
    # lg.error.assert_called_once_with(*args)
    # evt.set()
