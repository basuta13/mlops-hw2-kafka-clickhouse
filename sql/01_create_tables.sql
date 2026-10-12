CREATE DATABASE IF NOT EXISTS mlops;

CREATE TABLE IF NOT EXISTS mlops.transactions_raw
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

CREATE TABLE IF NOT EXISTS mlops.transactions_kafka
AS mlops.transactions_raw
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:19092',
    kafka_topic_list = 'transactions',
    kafka_group_name = 'clickhouse_transactions',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536,
    kafka_flush_interval_ms = 1000,
    kafka_skip_broken_messages = 0;

CREATE MATERIALIZED VIEW IF NOT EXISTS mlops.transactions_mv
TO mlops.transactions_raw
AS SELECT
    row_id,
    transaction_time,
    merch,
    cat_id,
    amount,
    name_1,
    name_2,
    gender,
    street,
    one_city,
    us_state,
    post_code,
    lat,
    lon,
    population_city,
    jobs,
    merchant_lat,
    merchant_lon,
    target
FROM mlops.transactions_kafka;
