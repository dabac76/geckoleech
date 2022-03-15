import os
from datetime import datetime as dt
from pycoingecko import CoinGeckoAPI
from geckoleech.main import APIReq, leech

# FIRST RUN IN TERMINAL: gecko db-ddl -f ./tests/example.sql
os.chdir("..")
cg = CoinGeckoAPI()


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


# noinspection SqlResolve
cg_exchanges = APIReq(
    "CoinGecko: /exchanges | Page: 1",
    cg.get_exchanges_list,
    None,
    [good_rank_newest_exchanges],
    ["insert into main.exchange values (?, ?, ?, ?)"]
)


# noinspection SqlResolve
cg_market = APIReq(
    "CoinGecko: /coins/{id}/history",
    cg.get_coin_history_by_id,
    ([{"bitcoin", "ethereum"}, {"01-03-2022", "02-03-2022"}], {"localization": False}),
    [aed_cny_rub_march_gecko_price, twitter_reddit_socials],
    ["insert into main.market_history values (?, ?, ?, ?, ?, ?)", "insert into main.social values (?, ?, ?, ?)"]
)

if __name__ == "__main__":
    leech()
