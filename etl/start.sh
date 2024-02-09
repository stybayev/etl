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

# Запуск основного ETL процесса
exec python main.py
