from threading import Lock
import concurrent.futures
import random
import logging
from time import sleep
from dataclasses import dataclass
from typing import ClassVar, List, Callable, Iterator, Optional, Union
from queue import Queue, Empty
from pyjsonq import JsonQ
from geckoleech.utils import DuckDB, expand

REQ_DELAY = 1.5
LOG_PATH = "geckoleech.log"
logging.basicConfig(filename=LOG_PATH, filemode="w", level=logging.ERROR)


@dataclass
class APIReq:
    _leeches: ClassVar[List["APIReq"]] = []
    name: str
    req: Callable
    params: Optional[tuple[List[Optional[set]], Optional[dict]]]
    json_query: Callable[[JsonQ | dict], Union[List[List | tuple], Iterator[List]]]
    sql_query: str

    def __post_init__(self):
        APIReq._leeches.append(self)

    def __str__(self):
        return f"{self.name}"

    @classmethod
    def all(cls) -> list["APIReq"]:
        return cls._leeches


def _task(req: APIReq, q: Queue):
    sleep(random.choice([n/2 for n in range(1, 20)]))
    for args_kwargs in expand(req.params):
        try:
            if not args_kwargs:
                resp = req.req()
            elif len(args_kwargs) == 1:
                resp = req.req(*args_kwargs[0])
            else:
                resp = req.req(*args_kwargs[0], **args_kwargs[1])
        except Exception as e:
            logging.error("REQUEST Error: %s | %s", str(args_kwargs), str(e))
            continue
        else:
            q.put((req.sql_query, req.json_query, resp))
            sleep(REQ_DELAY)


# Retrying is responsibility of api call (pycoingecko tries 5x hardcoded)
# Pagination also (is paid feature in CoinGeckoAPI pro)
def leech():
    q = Queue()
    lock = Lock()
    reqs = APIReq.all()
    in_process = len(reqs)

    # noinspection PyUnusedLocal
    def cnt_progress(future):
        nonlocal lock, in_process
        with lock:
            in_process -= 1

    # JSON Producers
    with concurrent.futures.ThreadPoolExecutor() as exe:
        futures = {
            exe.submit(_task, obj, q): obj.name
            for obj in reqs
        }
        for fut in futures:
            futures[fut].add_done_callback(cnt_progress)

        with DuckDB() as db:
            while in_process > 0 or not q.empty():
                try:
                    sql_query, json_query, resp = q.get()
                except Empty:
                    continue
                else:
                    # JSON Query has to return
                    # Union[List[List | tuple], Iterator[List]]
                    # in order for DuckDb.con.executemany to work
                    row_gen = json_query(JsonQ(data=resp))
                    db.executemany(sql_query, row_gen)
