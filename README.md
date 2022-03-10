
## GeckoLeech
###### A tiny utility to help quickly consume data from REST APIs into analytical database.

---

Inspired by a simple use case of gathering data from CoinGecko, although it can be used with any kind of json rest api. However, GeckoLeech is not directly accessing rest service. User is expected to provide a callable that handles the request and returns a json response. 

Purpose is strictly prototyping not production. It removes the burden of handling looping and database access, nothing more. Authenticated requests and response pagination are expected to be handled by user. The code doesn't even have requests/urllib as a dependency. As said, it expects a callable that handles whole logic of a rest api request. Each request is executed in its own thread. Project dependencies: DuckDB, pyjsonq, click. User should provide in advance: 

- database model, 
- hint to all combinations of api request parameters and 
- json query (in `pyjsonq` flavour), how to transform the response prior to database insertion

Destination is [DuckDB](https://duckdb.org/docs/api/python), a single file database, which provides low latency columnar access to data without running in separate process. It is Apache Arrow compliant and has direct fetching of data to numpy array or pandas data frame with `conn.execute("select ...").fetchnumpy()` or `.fetchdf()`.

### Installation

---

Assuming you have poetry installed: clone the repo, inside the repo dir run `poetry shell` to get virtualenv and then `poetry install --no-dev`.

### Usage

---

1. Before anything else, create necessary database tables. `gecko` command is exposed to help with that:
```commandline
gecko ddb-dl -s "CREATE TABLE ..."
```
```commandline
gecko ddb-dl -f <path-to-file-with-sql-statement>
```
2. Inside your script, import two objects from the main module: `from geckoleech.main import APIReq, leech`. 
3. Instantiate any number of `APIReq` objects and then call `leech()` function. 
 
To instantiate `APIReq` object, user needs to provide the following:

```python
from dataclasses import dataclass
from typing import List, Callable, Iterator, Optional, Union
from pyjsonq import JsonQ


@dataclass()
class APIReq:
    name: str  #Just for reporting purpose.
    req: Callable  #Performs api requests by using all combinations of params as args/kwargs
    params: Optional[tuple[List[Optional[set]], Optional[dict]]] # None or tuple(list, dict)
    json_queries: List[Callable[[JsonQ | dict], Union[List[List | tuple], Iterator[List]]]]  #pyjsonq flavour.
    sql_queries: List[str]  #Insertion query.

```
For example: for a given callable `req: get_data` and `params` given as:

```python
([{"date1", "date2"}, {1, 2}, 3], {"key1": "val1", "key2": {5, 6}})
```
Then callable will be called the following number of times and with the following argument combinations:

```python
get_data("date1", 1, 3, key1="val1", key2=5)
get_data("date2", 1, 3, key1="val1", key2=5)
get_data("date1", 2, 3, key1="val1", key2=5)
get_data("date2", 2, 3, key1="val1", key2=5)

get_data("date1", 1, 3, key1="val1", key2=6)
get_data("date2", 1, 3, key1="val1", key2=6)
get_data("date1", 2, 3, key1="val1", key2=6)
get_data("date2", 2, 3, key1="val1", key2=6)
```
So, all arguments / keyword arguments to callable `req` _**provided to `params` inside of a set**_ will be expanded into different calls with **_all possible combinations of each_**. 

Module `utils` has a convenience function `utils.dates(start, end, dformat)` that returns a set with all dates within a desired date interval.

### Example

---

To further explain the `params` argument in `APIReq` constructor, let's take an example:

You would like to consume the following data from CoinGecko's service: get the list of all supported coins into one database table and get bitcoin's historical price data for all markets during the last month into another database table.

