from collections import namedtuple
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
    args = ("%s | %s", ("once",), "ConnError")
    # with pytest.raises(Exception) as exc:
    #     gl._task(req)
    main._task(req)
    lg.error.assert_called_once_with(*args)


# noinspection SqlResolve
@patch("geckoleech.main._task")
@patch.object(main.APIReq, "all")
def test_concurrent_ddb(all_resp, task, ddb_cursor):
    APIResp = namedtuple("APIResp", "data name")
    all_resp.return_value = [APIResp([("shitcoin0001", 2), ("shitcoin0002", 4)], "TASK1"),
                             APIResp([("shitcoin0003", 8), ("shitcoin0004", 16)], "TASK2"),
                             APIResp(ValueError("Thread exception case"), "TASK3")]
    task.side_effect = lambda r: \
        ddb_cursor.executemany("INSERT INTO main.tst VALUES (?, ?);", r.data)
    main.leech()
    ddb_cursor.execute("SELECT * FROM main.tst;")
    actuals = ddb_cursor.fetchall()
    expected = [*all_resp.return_value[0].data, *all_resp.return_value[1].data]
    assert all([actual in expected for actual in actuals])
