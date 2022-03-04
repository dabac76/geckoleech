
## GeckoLeech
###### A tiny utility to help quickly consume data from REST APIs into analytical database.

---

Inspired by a simple use case of gathering data from CoinGecko, although it can be used with any kind of json rest api. However, GeckoLeech is not directly accessing rest service. User is expected to provide a callable that handles the request and returns a json response. 

Purpose is strictly prototyping not production. It removes the burden of handling looping and database access, nothing more. Authenticated requests and response pagination are expected to be handled by user. The code doesn't even have requests/urllib as a dependency. It expects a callable that handles whole logic of a rest api request. Each request is executed in its own thread. Project dependencies: DuckDB, pyjsonq, click. User should provide in advance: 

- database model, 
- hint to all combinations of api request parameters and 
- json query (in `pyjsonq` flavour), how to transform the response prior to database insertion

Destination is [DuckDB](https://duckdb.org/docs/api/python), a single file database, which provides low latency columnar access to data without running in separate process. It is Apache Arrow compliant and has direct fetching of data to numpy array or pandas data frame with `conn.execute("select ...").fetchnumpy()` or `.fetchdf()`.

### Installation

---

Assuming you have poetry installed: clone the repo, inside the repo dir run `poetry shell` to get virtualenv and then `poetry install --no-dev`.

### Usage

---

 Before anything else, create necessary database tables. You need to import two objects from the main module: `from geckoleech.main import APIReq, leech`. Then instantiate any number of `APIReq` objects and then call `leech()` function. 
 
For instantiating `APIReq` object, user needs to provide the following:

```python
class APIReq:
    name: str  #Just for reporting purpose.
    req: Callable  #Performs api requests by using all combinations of params as args/kwargs
    params: Optional[tuple[List[Optional[set]], Optional[dict]]]
    json_query: Callable[[JsonQ | dict], Union[List[List | tuple], Iterator[List]]]  #pyjsonq flavour.
    sql_query: str  #Insertion query.

```

For example: if callable `get_data` is given as a `req` and if `params` which has to be given in form `([], {})` is given as:

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
So, all arguments / keyword arguments to callable `req` that are provided to `params` inside of a set will be expanded into different calls with **all possible combinations of each**. 

Module `utils` has a convenience function `utils.dates(start, end, dformat)` that returns a set with all dates within a desired date interval.

To further explain the `params` argument, let's take an example:

You would like to consume the following data from CoinGecko's service: get the list of all supported coins into one database table and get bitcoin's historical price data for all markets during the last month into another database table.

