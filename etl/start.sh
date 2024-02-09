#!/bin/bash

set -euo pipefail

# Дождемся, пока база данных будет готова
while ! nc -z $POSTGRES_HOST $POSTGRES_PORT; do
  echo "Waiting for the database at $POSTGRES_HOST:$POSTGRES_PORT..."
  sleep 0.1
done
echo "Database $POSTGRES_HOST:$POSTGRES_PORT is up and running!"

echo "Running ..."
python main.py


