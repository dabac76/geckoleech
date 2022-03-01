
## GeckoLeech
###### A tiny utility to help experiment with REST APIs.

---

Inspired by a simple use case of gathering data from CoinGecko, although it can be used with any kind of json rest api. Purpose is strictly prototyping not production.  It removes the burden of handling looping, requests and database access nothing more. Authenticated requests and response pagination are not supported. User should provide in advance: 

- database model, 
- all combinations of api request parameters and 
- json query (using `pyjsonq` library) to transform the response prior to database insertion

Code is relying on [DuckDB](https://duckdb.org/docs/api/python), which provides low latency columnar access to data without running in separate process. It is Apache arrow complaint and has direct fetching of data to numpy array or pandas frame (`conn.execute("select ...").fetchnumpy()` or `.fetchdf()`).

### Usage

---

