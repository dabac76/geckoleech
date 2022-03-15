
## GeckoLeech
###### A tiny utility to help quickly consume data from REST APIs into analytical database.

---

Inspired by a simple use case of gathering data from CoinGecko, although it can be used with any kind of json rest api. GeckoLeech, however, is not directly accessing rest service. User is expected to provide a callable that handles the request and returns a dict response. 

Purpose is strictly prototyping not production. It removes the burden of handling looping and database access, nothing more nothing less. Authenticated requests and response pagination are expected to be handled by provided callable. The code doesn't even have requests/urllib as a dependency. As said, it expects a callable that handles whole logic of a rest api request. Each request is executed in its own thread (up to 32 in pool). Project dependencies: DuckDB, pyjsonq, click. User should provide in advance: 

- database model, 
- request callable
- hint to all combinations of api request parameters and 
- json query (in `pyjsonq` flavour), how to transform the response prior to database insertion.

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
2. Inside your script, import two objects (class and function) from the main module:
    ```python
    from geckoleech.main import APIReq, leech
   ```
3. Instantiate any number of `APIReq` objects and then call `leech()` function (guarded with `if __name__ == "__main__"`). 
 
To instantiate `APIReq` object, user needs to provide the following:

```python
@dataclass()
class APIReq:
    name: str  # Just for reporting purpose.
    req: Callable  # Performs api requests by using all combinations of params as args/kwargs
    params: Optional[tuple[List[Optional[set]], Optional[dict]]]  # None or tuple(list, dict)
    json_queries: List[Callable[[JsonQ | dict], Union[List[List | tuple], Iterator[List]]]]  # list of pyjsonq flavour queries.
    sql_queries: List[str]  # List of insertion queries, corresponding to json queries.

```

Example for `req` and `params` attributes: for a given callable `req: get_data` and `params` given as:

```python
([{"date1", "date2"}, {1, 2}, 3], {"key1": "val1", "key2": {5, 6}})
```

Callable will be called with:

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

Therefore, all arguments / keyword arguments for callable `req` _**provided to**_ `params` _**inside of a set**_ will be expanded into different calls with **_all possible combinations of each_**. 

Module `utils` has a convenience function `utils.dates(start, end, dformat)` that returns a set with all dates within a desired date interval.

Inside virtualenv shell you have a command line interface (`gecko --help`) exposed with 3 commands:

* `gecko db-ddl --help` to execute arbitrary ddl statements like create tables.
* `gecko db-remove` to delete all rows from wanted tables.
* `gecko db-stat` to report a few database stats.
* `gecko dry-run` to see all the argument combinations with which your requests will be called.

### Example

---

You would like to consume the following data from CoinGecko's service: 
* Get the list of all exchanges, opened in 2019 and 2020 with trust score > 80. 
* Get CoinGecko's btc/eth price/volume/market cap data for 2022-03-01 and 2022-03-02 in rub/cny/aed/sar/inr currency units.
* Get btc/eth number of Twitter followers and average reddit posts for 2022-03-01 and 2022-03-02.

As mentioned we'll handle this in two steps: create data model, create script with `APIReq` objects with all json transformations and database insertion statements and run it with `leech()`.

##### Datamodel

For the above requirements the adequate data model is (checkout [DuckDB](https://duckdb.org/docs/) sql dialect:): 

```sql
-- ./tests/example.sql
create table if not exists main.exchange (
    id varchar,
    url varchar,
    year integer,
    trust_score integer
);
create table if not exists main.market_history (
    ext_date date,
    id varchar,
    currency varchar,
    price decimal(10,2),
    mcap ubigint,
    volume ubigint
);
create table if not exists main.social (
    ext_date date,
    id varchar,
    tw_follows integer,
    reddit_avg_post FLOAT
);
```

Create the database with the help of the exposed command line interface: `gecko db-ddl -f ./tests/example.sql`. 

##### Json Queries and the Rest of Script

Next step is to find how to properly transform the api response. This will require some effort in python console to come up with the correct [pyjsonq](https://github.com/s1s1ty/py-jsonq) expression. In this case, here are the 3 callables to transform the json responses in accordance to the data model:

```python
def good_rank_newest_exchanges(response, request):

    # Ignore request's calling args/kwargs.
    _ = request
    # This is just how pyjsonq library works.
    response.reset()
    query = response.at('.')\
        .where('trust_score_rank', '>', 80)\
        .where('year_established', 'in', [2019, 2020])\
        .sort_by('trust_score_rank', 'desc')\
        .get()

    # Let's return generator just to show it's a possibility.
    return ([d["id"], d["url"], d["year_established"], d["trust_score_rank"]] for d in query)


def aed_cny_rub_march_gecko_price(response, request):

    # CoinGecko's response unfortunately doesn't have a date requested.
    # Fortunately, geckoleech allows you to access all request args/kwargs as well.
    datestamp = dt.strptime(request[0][1], "%d-%m-%Y").strftime('%Y-%m-%d')

    response.reset()
    coin_id = response.at('id').get()

    response.reset()
    query_price = response.at('market_data.current_price').get()
    response.reset()
    query_mcap = response.at('market_data.market_cap').get()
    response.reset()
    query_vol = response.at('market_data.total_volume').get()

    return \
        [(datestamp, coin_id, curr, query_price[curr], query_mcap[curr], query_vol[curr])
         for curr in ["aed", "cny", "rub", "sar", "inr"]]


def twitter_reddit_socials(response, request):

    datestamp = dt.strptime(request[0][1], "%d-%m-%Y").strftime('%Y-%m-%d')

    response.reset()
    coin_id = response.at('id').get()

    response.reset()
    query = response.at('community_data').get()

    # Even if only one row is returned, DuckDB's executemany() still demands a list not tuple
    return [(datestamp, coin_id, query["twitter_followers"], query["reddit_average_posts_48h"])]
```

First one gives the row for the exchanges table, second one rows for the market_history table and the third for table social. Notice the signature of the json query function: response (the JsonQ type) and request (value of api request arguments are also available). Also note that you can return a generator as well. This way you can make arbitrary complex json transformations including multi-pass and aggregations previous to insertion to database.

Putting it all together now inside the `APIReq` objects leads to the rest of the script:

```python
cg_exchanges = APIReq(
    "CoinGecko: /exchanges | Page: 1",
    cg.get_exchanges_list,
    None,
    [good_rank_newest_exchanges],
    ["insert into main.exchange values (?, ?, ?, ?)"]
)


cg_market = APIReq(
    "CoinGecko: /coins/{id}/history",
    cg.get_coin_history_by_id,
    ([{"bitcoin", "ethereum"}, {"01-03-2022", "02-03-2022"}], {"localization": False}),
    [aed_cny_rub_march_gecko_price, twitter_reddit_socials],
    ["insert into main.market_history values (?, ?, ?, ?, ?, ?)", "insert into main.social values (?, ?, ?, ?)"]
)

if __name__ == "__main__":
    leech()
```
Notice in `cg_market` how you can use one response multiple times. Just be aware of ordering in lists. Index positions of json query and its corresponding database insert statement have to be the same.

If you want to make sure how the requests (`get_exchange_list` and `get_coin_history_by_id`) are called use the exposed command: `gecko dry-run -f ./tests/example.py`.

Now the only thing left for you to have the result in pandas is:

```python
con = duckdb.connect(database='geckoleech.db', read_only=True)
df = con.execute("SELECT * FROM <table_name>").fetchdf()
```
