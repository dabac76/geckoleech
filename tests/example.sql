--
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
