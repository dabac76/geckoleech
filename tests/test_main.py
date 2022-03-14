from collections import namedtuple
from unittest.mock import Mock, MagicMock, patch, PropertyMock
from pyjsonq import JsonQ
import duckdb
import geckoleech.main as main


def test_class_counter():

    gco1 = main.APIReq("gc01", Mock(), Mock(), MagicMock(), MagicMock())
    gco2 = main.APIReq("gc02", Mock(), Mock(), MagicMock(), MagicMock())
    assert main.APIReq.all() == [gco1, gco2]

    # Safe tear-down if running all the tests at once
    main.APIReq._leeches = []


@patch("geckoleech.main.logging")
@patch("geckoleech.main.APIReq")
def test_task_log(req, lg):
    # _task(arg) function test

    req.name = "ExceptionTest"
    req.req.side_effect = Exception("ConnError")
    args = ("REQUEST: %s -> %s | %s", req.name, str(None), "ConnError")
    main._task((req, None, 0))
    lg.error.assert_called_once_with(*args)


# noinspection SqlResolve
@patch("geckoleech.main._task")
@patch.object(main.DuckDB, "DB_PATH", new_callable=PropertyMock)
@patch.object(main.APIReq, "all")
def test_leech(apireq_all, db, task, ddb_prepare, capsys):
    # leech() function test

    db.return_value = "test.db"

    response1 = {"InsolventX": [{"name": "shitcoin1", "price": 2},
                                {"name": "shitcoin2", "price": 4}]}
    response2 = {"InsolventX": [{"name": "shitcoin3", "price": 8},
                                {"name": "shitcoin4", "price": 16}]}

    sql = "INSERT INTO main.tst VALUES (?, ?);"

    def json_query(js: JsonQ, ignore):
        _ = ignore
        jq = js.at("InsolventX").where("price", ">", 0).get()
        return [(el["name"], el["price"]) for el in jq]

    ReqResp = namedtuple("ReqResp", "name params json_queries sql_queries resp")
    rs1 = ReqResp("simulated:req1&resp1", None, [json_query], [sql], response1)
    rs2 = ReqResp("simulated:req2&resp2", None, [json_query], [sql], response2)
    rs3 = ReqResp("simulated:req3&None", None, [json_query], [sql], None)
    apireq_all.return_value = [rs1, rs2, rs3]

    task.side_effect = lambda tup: (tup[0], tup[1], tup[0].resp)

    main.leech()

    captured = capsys.readouterr()

    # Mystery: If DuckDb conn cursor is passed to test function from
    # conftest.py as a coroutine, then this query returns empty list !?!?
    con = duckdb.connect("test.db", read_only=True)
    con.execute("SELECT * FROM main.tst;")
    actuals = con.fetchall()
    con.close()
    expected = json_query(JsonQ(data=response1), None) + json_query(JsonQ(data=response2), None)
    assert all([actual in expected for actual in actuals])
    assert f"SUCCESS: {rs1.name} -> {rs1.params}" in captured.out
    assert f"SUCCESS: {rs2.name} -> {rs2.params}" in captured.out
    assert f"NO_RESPONSE_DETAILS_LOGGED: {rs3.name} -> {rs3.params}" in captured.out
