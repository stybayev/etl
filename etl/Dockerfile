FROM python:3.11-slim-buster

# Environment variables
ENV PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=random \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    POETRY_VERSION=1.6.1 \
    POETRY_VIRTUALENVS_CREATE=false \
    POETRY_CACHE_DIR='/var/cache/pypoetry'

# System dependencies and cleanup
RUN apt-get update -q && \
    apt-get install -q --no-install-recommends -y build-essential gettext libpq-dev wget netcat curl && \
    apt-get autoremove -y && apt-get clean -y && rm -rf /var/lib/apt/lists/* && \
    pip install "poetry-core==1.7.0" "poetry==$POETRY_VERSION" && poetry --version

# Set working directory
WORKDIR /code

# Copy project files (pyproject.toml and poetry.lock first for caching)
COPY ./pyproject.toml ./poetry.lock* /code/

# Install all dependencies
RUN poetry install --no-root

# Copy the rest of the project files
COPY . /code/

COPY start.sh /start.sh
RUN chmod +x /start.sh

COPY create_index.sh /create_index.sh
RUN chmod +x /create_index.sh
