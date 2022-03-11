from collections import namedtuple
from queue import Queue
from unittest.mock import Mock, MagicMock, patch, PropertyMock
from pyjsonq import JsonQ
import duckdb
import geckoleech.main as main


def test_class_counter():
    gco1 = main.APIReq("gc01", Mock(), Mock(), MagicMock(), MagicMock())
    gco2 = main.APIReq("gc02", Mock(), Mock(), MagicMock(), MagicMock())
    assert main.APIReq.all() == [gco1, gco2]

    # For the case when running all the tests
    main.APIReq._leeches = []


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
@patch("geckoleech.main._task")
@patch.object(main.DuckDB, "DB_PATH", new_callable=PropertyMock)
@patch.object(main.APIReq, "all")
def test_leech(all_resp, db, task, ddb_prepare, capsys):
    db.return_value = "test.db"

    APIResp = namedtuple("APIResp", "data name")
    data1 = {"InsolventX": [{"name": "shitcoin1", "price": 2},
                            {"name": "shitcoin2", "price": 4}]}
    data2 = {"InsolventX": [{"name": "shitcoin3", "price": 8},
                            {"name": "shitcoin4", "price": 16}]}

    all_resp.return_value = [APIResp(data1, "TASK1"), APIResp(data2, "TASK2")]

    sql = "INSERT INTO main.tst VALUES (?, ?);"

    def json_query(js: JsonQ, ignore):
        _ = ignore
        jq = js.at("InsolventX").where("price", ">", 0).get()
        return [(el["name"], el["price"]) for el in jq]

    Req = namedtuple("Req", "name json_queries sql_queries")
    req = Req("simulated req", [json_query], [sql])
    task.side_effect = lambda rsp, q: q.put(("args_kwargs", req, rsp.data))

    main.leech()

    captured = capsys.readouterr()

    # Mystery: If DuckDb conn cursor is passed to test function from
    # conftest.py coroutine, then this query returns empty list !?!?
    con = duckdb.connect("test.db", read_only=True)
    con.execute("SELECT * FROM main.tst;")
    actuals = con.fetchall()
    con.close()
    expected = json_query(JsonQ(data=data1), []) + json_query(JsonQ(data=data2), [])
    assert all([actual in expected for actual in actuals])
    assert "SUCCESS: " in captured.out
