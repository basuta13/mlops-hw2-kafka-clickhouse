DROP TABLE IF EXISTS mlops.transactions_optimized;

CREATE TABLE mlops.transactions_optimized
(
    row_id UInt64,
    transaction_time DateTime('UTC'),

    merch LowCardinality(String),
    cat_id LowCardinality(String),

    amount Decimal(18, 2),

    name_1 String,
    name_2 String,

    gender LowCardinality(String),

    street String,
    one_city String,

    us_state LowCardinality(String),

    post_code String,

    lat Float64,
    lon Float64,

    population_city UInt32,

    jobs String,

    merchant_lat Float64,
    merchant_lon Float64,

    target UInt8
)
ENGINE = MergeTree
PRIMARY KEY us_state
ORDER BY (us_state, cat_id, amount);

INSERT INTO mlops.transactions_optimized
SELECT *
FROM mlops.transactions_raw;

OPTIMIZE TABLE mlops.transactions_optimized FINAL;
