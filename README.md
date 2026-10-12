# MLOps HW2: Kafka + ClickHouse

Домашняя работа по Kafka и ClickHouse.

## Что делает проект

Данные из `train.csv` загружаются в Kafka, после чего ClickHouse читает их из Kafka и сохраняет в таблицу через Materialized View.

Также есть SQL-запрос, который находит категорию самой большой транзакции для каждого штата.

## Структура проекта

    .
    ├── data/
    │   └── .gitkeep
    ├── results/
    │   └── max_transaction_by_state.csv
    ├── scripts/
    │   └── csv_to_kafka.py
    ├── sql/
    │   ├── 01_create_tables.sql
    │   ├── 02_max_transaction_by_state.sql
    │   └── 03_create_optimized_table.sql
    ├── docker-compose.yml
    ├── requirements.txt
    └── README.md

## Данные

Датасет:

https://www.kaggle.com/t/1918f3f6435300327d38d6c596f97394

## Запуск
Установить зависимости:
Он находит категорию самой большой транзакции для каждого штата.
После скачивания файл `train.csv` нужно положить сюда:

    data/train.csv


Создать виртуальное окружение:

    python3 -m venv .venv
    source .venv/bin/activate


    pip install -r requirements.txt

Запустить Kafka и ClickHouse:

    docker compose up -d

Загрузить данные из CSV в Kafka:

    python scripts/csv_to_kafka.py

После загрузки можно проверить количество строк в ClickHouse:

    docker compose exec -T clickhouse clickhouse-client --user mlops --password mlops_local --query "SELECT count() FROM mlops.transactions_raw"

Ожидаемое количество строк:

    786431

## SQL-запрос

Запрос находится в файле:

    sql/02_max_transaction_by_state.sql


Для сохранения результата в CSV:

    docker compose exec -T clickhouse clickhouse-client --user mlops --password mlops_local --format CSVWithNames < sql/02_max_transaction_by_state.sql > results/max_transaction_by_state.csv

Результат сохраняется в:

    results/max_transaction_by_state.csv

## Оптимизация

Изначально таблица использовала:

    ORDER BY tuple()

После оптимизации используются:

    PRIMARY KEY us_state
    ORDER BY (us_state, cat_id, amount)

Для категориальных колонок также используется `LowCardinality(String)`.

В benchmark QPS вырос примерно с 37.1 до 45.8.

## Остановка

    docker compose down
