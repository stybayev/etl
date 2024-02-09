#!/bin/bash

set -e

# Ожидание доступности Postgres
echo "Waiting for Postgres to be ready..."
while ! nc -z db 5432; do
  sleep 0.1
done

echo "Postgres is ready!"

# Запуск основного ETL процесса
exec python main.py
