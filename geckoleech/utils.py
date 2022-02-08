import datetime as dt
from itertools import product
import duckdb


DB_PATH = "geckoleech.db"


class DuckDB:

    conn = duckdb.connect(database=DB_PATH, read_only=False)

    def __init__(self):
        self.cursor = DuckDB.conn.cursor()

    def __enter__(self):
        return self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()


def dates_cg_format(start, end=None):
    start = dt.datetime.strptime(start, "%d-%m-%Y").date()
    if end is None:
        end = dt.datetime.now().date()
    else:
        end = dt.datetime.strptime(end, "%d-%m-%Y").date()
    return {(start + dt.timedelta(days=dx)).strftime("%d-%m-%Y")
            for dx in range((end - start).days + 1)}


def expand(args_kwargs):
    # Validate if input is of ([], {}) form
    if not args_kwargs:
        return [None]
    elif type(args_kwargs[0]) is not list:
        raise ValueError("1st element of params tuple has to be list")
    args = args_kwargs[0]
    try:
        kwargs = args_kwargs[1]
    except IndexError:
        kwargs = {}
    else:
        if type(kwargs) is not dict:
            raise ValueError("2nd element of params tuple has to be dict")
    # Expand all combinations of given positional arguments
    combs = [arg for arg in args if type(arg) is set]
    scalars = tuple(arg for arg in args if type(arg) is not set)
    args_expanded = [scalars + elem for elem in product(*combs)]
    if not kwargs:
        return [(el,) for el in args_expanded]
    # Expand all combinations of given keyword arguments
    # (Only keys having sets associated to them will expand to all combinations)
    kwargs_expanded = None
    try:
        keys, sets = zip(*[(k, v) for k, v in kwargs.items() if type(v) is set])
    except ValueError:
        kwargs_expanded = [kwargs]
    else:
        # Is there ordering guarantee from product? It says it guarantees
        # "lexicographic" ordering. If meant "lexicon" is the order of inputs
        # given to product, then it will preserve the position
        # inside resulting tuples.
        combs = [item for item in product(*sets)]
        combs = [{keys[i]: vals[i] for i in range(len(keys))} for vals in combs]
        # Concatenate with the rest of items having atomic values
        scalars = {k: v for k, v in kwargs.items() if type(v) not in (list, tuple, set)}
        kwargs_expanded = [{**d, **scalars} for d in combs]
    finally:
        # Combine with expanded positional args
        return list(product(args_expanded, kwargs_expanded))


def save2db(query, gen):
    with DuckDB() as db:
        db.executemany(query, gen)
        db.commit()
