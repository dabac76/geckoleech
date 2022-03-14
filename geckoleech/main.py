from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from time import sleep
from dataclasses import dataclass
from typing import ClassVar, List, Callable, Iterator, Optional, Union
from pyjsonq import JsonQ
from geckoleech.utils import DuckDB, expand

REQ_TIME_DELAY = 0.5
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
        :param json_queries: Callable to transform api response prior to insertion to database.
        You may use pyjsonq flavour query. Should return either iterable or iterator.
        :type json_queries: Callable[[JsonQ | dict], Union[List[List | tuple], Iterator[List]]]
        :param sql_queries: Insertion query in DuckDB sql. Specify how to insert the result of previous json query.
        :type sql_queries: str

    :Note:
    After instantiating all desired APIReq object call leech() to start...well...
    leeching.
    """
    _leeches: ClassVar[List["APIReq"]] = []
    name: str
    req: Callable
    params: Optional[tuple[List[Optional[set]], Optional[dict]]]
    json_queries: List[Callable[[JsonQ | dict, List], Union[List[List | tuple], Iterator[List]]]]
    sql_queries: List[str]

    def __post_init__(self):
        assert len(self.json_queries) == len(self.sql_queries)
        APIReq._leeches.append(self)

    def __str__(self):
        return f"{self.name}"

    @classmethod
    def all(cls) -> list["APIReq"]:
        return cls._leeches


def _task(arg):

    req, args_kwargs, seconds = arg
    sleep(seconds)
    # All exception handling has to be done within request callable.
    # Including timeouts.
    resp = None
    try:
        if not args_kwargs:
            resp = req.req()
        elif len(args_kwargs) == 1:
            resp = req.req(*args_kwargs[0])
        else:
            resp = req.req(*args_kwargs[0], **args_kwargs[1])
    except Exception as e:
        logging.error("REQUEST: %s -> %s | %s", req.name, str(args_kwargs), str(e))
    finally:
        return req, args_kwargs, resp


# Retrying is responsibility of api call (pycoingecko tries 5x hardcoded)
# Pagination also (is paid feature in CoinGeckoAPI pro)
def leech():

    tasks = []
    seconds = 0
    for req in APIReq.all():
        for args_kwargs in expand(req.params):
            tasks.append((req, args_kwargs, seconds))
            seconds += REQ_TIME_DELAY

    with ThreadPoolExecutor(32) as exe:
        # JSON Producing threads.
        futures = [exe.submit(_task, arg) for arg in tasks]

        # Database consumer. DuckDB access has to be in single thread only.
        with DuckDB() as db:
            for future in as_completed(futures):
                # DuckDb.con.executemany function demands
                # JSON query to return either iterable or iterator:
                # Union[List[List | tuple], Iterator[List]]
                # So if a single record is returned (tuple),
                # it has to be enclosed in a list.
                result = future.result()
                req, args_kwargs, resp = result
                if not resp:
                    print(f"NO_RESPONSE_DETAILS_LOGGED: {req.name} -> {args_kwargs}")
                    continue
                for query_pair in zip(req.json_queries, req.sql_queries):
                    row_gen = query_pair[0](JsonQ(data=resp), args_kwargs)
                    db.executemany(query_pair[1], row_gen)
                print(f"SUCCESS: {req.name} -> {args_kwargs}")
