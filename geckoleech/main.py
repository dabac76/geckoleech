import concurrent.futures
import random
import logging
from time import sleep
from dataclasses import dataclass
from typing import ClassVar, List, Callable, Iterator, Optional
from pyjsonq import JsonQ
from geckoleech.utils import save2db, expand

REQ_DELAY = 1.5
LOG_PATH = "geckoleech.log"
logging.basicConfig(filename=LOG_PATH, filemode="w", level=logging.ERROR)


@dataclass
class APIReq:
    _leeches: ClassVar[List["APIReq"]] = []
    name: str
    req: Callable
    params: tuple[List[Optional[set]], Optional[dict]]
    json_query: Callable[[JsonQ], Iterator[List]]
    sql_query: str

    def __post_init__(self):
        APIReq._leeches.append(self)

    def __str__(self):
        return f"{self.name}"

    @classmethod
    def all(cls) -> list["APIReq"]:
        return cls._leeches


def _task(req: APIReq):
    sleep(random.choice([n/2 for n in range(1, 20)]))
    for args_kwargs in expand(req.params):
        try:
            resp = req.req(*args_kwargs[0], **args_kwargs[1])
        except Exception as e:
            logging.error("%s | %s", args_kwargs, e)
            continue
        else:
            row_gen = req.json_query(JsonQ(resp))
            save2db(req.sql_query, row_gen)
            sleep(REQ_DELAY)


# Retrying should be handled inside api call (pycoingecko in this case has
# hardcoded 5)
# Pagination. Pagination handler is a paid feature in CoinGeckoAPI pro.
# TODO: Implement cli: --db-status and --filter-request commands
# TODO: --dry-run to validate APIReq call parameters
def leech():
    with concurrent.futures.ThreadPoolExecutor() as exe:
        futures = {
            exe.submit(_task, obj): obj.name
            for obj in APIReq.all()
        }
        for fut in concurrent.futures.as_completed(futures):
            e = fut.exception()
            if e:
                print(f"FAILED {futures[fut]} :")
                print(e)
            else:
                print(f"SUCCESS: {futures[fut]}")
