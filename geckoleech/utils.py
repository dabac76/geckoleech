import datetime as dt
from itertools import product
import duckdb


class DuckDB:

    DB_PATH = "geckoleech.db"

    def __init__(self, read_only=False):
        self.conn = duckdb.connect(database=DuckDB.DB_PATH, read_only=read_only)

    def __enter__(self):
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()


def dates(start, end=None, dformat="%d-%m-%Y"):
    """Returns set of all dates within desired interval in a given format.

    :param start: Start of interval given as string in dformat.
    :type start: str
    :param end: Optional, end of interval given as string in dformat.
        (default None). If not given end is today's date.
    :type end: str
    :param dformat: Optional. Default: "%d-%m-%Y". Used to parse date strings.
    :returns: A set of dates as strings in dformat for each day within interval.
    :rtype: set
    """
    start = dt.datetime.strptime(start, dformat).date()
    if end is None:
        end = dt.datetime.now().date()
    else:
        end = dt.datetime.strptime(end, dformat).date()
    return {(start + dt.timedelta(days=dx)).strftime(dformat)
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
