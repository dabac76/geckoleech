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


# noinspection PyUnresolvedReferences,GrazieInspection
@dataclass
class APIReq:
    """
    Specify: REST API request, transformation of json response and insertion to DuckDB.

        :param name: Arbitrary name for diagnostic/reporting purpose. Will be printed to stdout during run.
        :type name: str
        :param req: Callable that encloses whole rest api request logic including retrying, pagination, ...
        Expected to return dictionary.
        :type req: Callable
        :param params: Request args/kwargs passed to callable.
        When given inside a set will be expanded to all possible combinations.
        :type params: Optional[tuple[List[Optional[set]], Optional[dict]]]
        :param json_query: Callable to transform api response prior to insertion to database.
        You may use pyjsonq flavour query. Should return either iterable or iterator.
        :type json_query: Callable[[JsonQ | dict], Union[List[List | tuple], Iterator[List]]]
        :param sql_query: Insertion query in DuckDB sql. Specify how to insert the result of previous json query.
        :type sql_query: str

    :Note:
    After instantiating all desired APIReq object call leech() to start...well...
    leeching.
    """
    _leeches: ClassVar[List["APIReq"]] = []
    name: str
    req: Callable
    params: Optional[tuple[List[Optional[set]], Optional[dict]]]
    json_query: List[Callable[[JsonQ | dict], Union[List[List | tuple], Iterator[List]]]]
    sql_query: List[str]

    def __post_init__(self):
        assert len(self.json_query) == len(self.sql_query)
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
            q.put((args_kwargs, req.sql_query, req.json_query, resp))
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
        for fut in futures.keys():
            fut.add_done_callback(cnt_progress)

        # Database consumer
        with DuckDB() as db:
            while in_process > 0 or not q.empty():
                try:
                    args_kwargs, sql_query, json_query, resp = q.get()
                except Empty:
                    continue
                else:
                    # DuckDb.con.executemany function demands
                    # JSON query to return either iterable or iterator:
                    # Union[List[List | tuple], Iterator[List]]
                    # So if a single record is returned (tuple),
                    # it has to be enclosed in a list.
                    for query_pair in zip(json_query, sql_query):
                        row_gen = query_pair[0](JsonQ(data=resp))
                        db.executemany(query_pair[1], row_gen)
                    print(f"SUCCESS: {args_kwargs}")
