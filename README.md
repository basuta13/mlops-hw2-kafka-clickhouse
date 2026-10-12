# MLOps HW2: Kafka + ClickHouse

Домашняя работа по Kafka и ClickHouse.

## Что делает проект

Данные из `train.csv` загружаются в Kafka, затем ClickHouse читает их из Kafka и сохраняет в таблицу.

Также есть SQL-запрос, который находит категорию самой большой транзакции для каждого штата.

## Структура проекта

```text
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
