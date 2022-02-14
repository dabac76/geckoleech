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
            try:
                # Query has to return
                # Union[List[List | tuple], Iterator[List]]]
                # in order for DuckDb.con.executemany to work
                row_gen = req.json_query(JsonQ(data=resp))
            except Exception as e:
                raise e
            else:
                q.put((req.sql_query, row_gen))
            sleep(REQ_DELAY)


def _cons(q: Queue, evt: threading.Event):
    with DuckDB() as db:
        print("enter")
        # while not evt.is_set():
        while True:
            try:
                data = q.get()
            except Empty:
                continue
            else:
                try:
                    # row_gen has to be of
                    # Union[List[List | tuple], Iterator[List]]]
                    print(data)
                    # db.executemany(*data)
                except Exception as exc:
                    logging.error("DUCKDB Error: %s | %s", data[0], str(exc))
                    continue


# Retrying is responsibility of api call (pycoingecko tries 5x hardcoded)
# Pagination also (is paid feature in CoinGeckoAPI pro)
def leech():
    q = Queue()
    evt = threading.Event()

    # Database consumer
    dbt = threading.Thread(target=_cons, args=(q, evt), daemon=True)
    dbt.start()

    # Json producers
    with concurrent.futures.ThreadPoolExecutor() as exe:
        futures = {
            exe.submit(_task, obj, q): obj.name
            for obj in APIReq.all()
        }

    print("\n")
    for fut in concurrent.futures.as_completed(futures):
        exc = fut.exception()
        if exc:
            print(f"JSONQuery Error: {futures[fut]}")
            print(exc)
        else:
            print(f"THREAD SUCCESS: {futures[fut]}")

    evt.set()
