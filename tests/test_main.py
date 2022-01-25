from unittest.mock import Mock, patch
import pytest
import geckoleech.main as gl


def test_class_counter():
    gco1 = gl.APIReq("gc01", Mock(), Mock(), Mock(), Mock())
    gco2 = gl.APIReq("gc02", Mock(), Mock(), Mock(), Mock())
    assert gl.APIReq.all() == [gco1, gco2]


@patch("geckoleech.main.sleep")
@patch("geckoleech.main.expand")
@patch("geckoleech.main.APIReq")
def test_task_raise(req, xp, sl):
    sl.side_effect = None
    xp.return_value = ["..discarded.."]
    req.req.side_effect = Exception("Conn.trouble")
    with pytest.raises(Exception) as exc:
        gl._task(req)
    assert "Conn.trouble" in str(exc.value)
