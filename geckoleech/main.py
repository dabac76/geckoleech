import threading
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
    json_query: Callable[[JsonQ | dict], Union[List, Iterator[List]]]
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
            logging.error("%s | %s", str(args_kwargs), str(e))
            continue
        else:
            row_gen = req.json_query(JsonQ(data=resp))
            q.put((req.sql_query, row_gen))
            sleep(REQ_DELAY)


# TODO: How to handle database exceptions??
def _cons(q: Queue):
    with DuckDB() as db:
        while True:
            try:
                data = q.get()
            except Empty:
                continue
            else:
                db.executemany(*data)


# Retrying is responsibility of api call (pycoingecko tries 5x hardcoded)
# Pagination also (is paid feature in CoinGeckoAPI pro)
def leech():
    q = Queue()

    dbt = threading.Thread(target=_cons, args=(q,), daemon=True)
    dbt.start()

    with concurrent.futures.ThreadPoolExecutor() as exe:
        futures = {
            exe.submit(_task, obj, q): obj.name
            for obj in APIReq.all()
        }

    print("\n")
    for fut in concurrent.futures.as_completed(futures):
        e = fut.exception()
        if e:
            print(f"THREAD FAILED: {futures[fut]}")
            print(e)
        else:
            print(f"THREAD SUCCESS: {futures[fut]}")
