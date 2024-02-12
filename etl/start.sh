#!/bin/bash

set -e

# Ожидание доступности Postgres
echo "Waiting for Postgres to be ready..."
while ! nc -z db 5432; do
  sleep 0.1
done
echo "Postgres is ready!"

# Ожидание доступности Elasticsearch
echo "Waiting for Elasticsearch to be ready..."
while ! nc -z elasticsearch 9200; do
  sleep 0.1
done
echo "Elasticsearch is ready!"


# Ожидание создания индекса в Elasticsearch
echo "Waiting for Elasticsearch index to be ready..."
INDEX_NAME="movies"
while true; do
    if curl -s -o /dev/null -w "%{http_code}" elasticsearch:9200/$INDEX_NAME | grep 200; then
        echo "Elasticsearch index $INDEX_NAME is ready!"
        break
    else
        echo "Waiting for index $INDEX_NAME to be created..."
        sleep 1
    fi
done

# Запуск основного ETL процесса
exec python main.py
