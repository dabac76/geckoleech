import datetime as dt
from itertools import product
import logging
import duckdb


DB_PATH = "geckoleech.db"


class DuckDB:

    conn = duckdb.connect(database=DB_PATH, read_only=False)

    def __init__(self):
        self.cursor = self.conn.cursor()

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


def expand(args, kwargs=None):
    if not kwargs:
        return list(product(*args))
    combs = None
    try:
        # Expand all combinations of kwargs
        keys, sets = zip(*[(k, v) for k, v in kwargs.items() if type(v) is set])
    except ValueError:
        combs = [kwargs]
    else:
        combs = [item for item in product(*sets)]
        # Expand keys with set values to all combinations of the value of each
        combs = [{keys[i]: vals[i] for i in range(len(keys))} for vals in combs]
        # Concatenate with the rest of items having atomic values
        scalars = {k: v for k, v in kwargs.items() if type(v) not in (list, tuple, set)}
        combs = [{**d, **scalars} for d in combs]
    finally:
        # Add to that all combinations of args
        return list(product(product(*args), combs))


def save2db(query, gen):
    with DuckDB() as db:
        db.executemany(query, gen)
        db.commit()
