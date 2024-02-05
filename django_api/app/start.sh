#!/bin/bash

set -euo pipefail

# Дождемся, пока база данных будет готова
while ! nc -z $DB_HOST $DB_PORT; do
  echo "Waiting for the database at $DB_HOST:$DB_PORT..."
  sleep 0.1
done
echo "Database $DB_HOST:$DB_PORT is up and running!"

python manage.py collectstatic --no-input
python manage.py migrate

echo "Running Gunicorn Server"
gunicorn --bind :8000 --timeout 90 --workers 2 config.wsgi:application
