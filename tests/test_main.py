from collections import namedtuple
from queue import Queue
from unittest.mock import Mock, patch
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
    args = ("%s | %s", str(("once",)), "ConnError")
    # with pytest.raises(Exception) as exc:
    #     gl._task(req, Queue())
    main._task(req, Queue())
    lg.error.assert_called_once_with(*args)


# noinspection SqlResolve
@patch("geckoleech.main._task")
@patch.object(main.DuckDB, "DB_PATH")
@patch.object(main.APIReq, "all")
def test_ddb_thread(all_resp, db, task, ddb_cursor, capsys):
    APIResp = namedtuple("APIResp", "data name")
    # all_resp.return_value = [APIResp([("shitcoin0001", 2), ("shitcoin0002", 4)], "TASK1"),
    #                          APIResp([("shitcoin0003", 8), ("shitcoin0004", 16)], "TASK2"),
    #                          APIResp(ValueError("Json query Error"), "TASK3")]
    all_resp.return_value = [APIResp([("shitcoin0001", 2), ("shitcoin0002", 4)], "TASK1"),
                             APIResp([("shitcoin0003", 8), ("shitcoin0004", 16)], "TASK2")]
    db.return_value = "test.db"
    task.side_effect = lambda r, q: \
        q.put(("INSERT INTO main.tst VALUES (?, ?);", r.data))
    main.leech()
    ddb_cursor.execute("SELECT * FROM main.tst;")
    captured = capsys.readouterr()
    actuals = ddb_cursor.fetchall()
    expected = [*all_resp.return_value[0].data, *all_resp.return_value[1].data]
    assert all([actual in expected for actual in actuals])
    print(captured.out)
    # assert "THREAD FAILED: TASK3" in captured.out
