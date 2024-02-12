from contextlib import contextmanager

import psycopg2
from pydantic_settings import BaseSettings
from pydantic.networks import PostgresDsn


class PostgresConfig(BaseSettings):
    """
    Конфигурация подключения к Postgres
    """
    host: str
    port: int
    dbname: str
    user: str
    password: str

    class Config:
        env_prefix = 'POSTGRES_'

    @property
    def dsn(self) -> str:
        return PostgresDsn.build(
            scheme='postgres',
            user=self.user,
            password=self.password,
            host=self.host,
            port=str(self.port),
            path=f'/{self.dbname}'
        )


class ElasticsearchConfig(BaseSettings):
    host: str

    class Config:
        env_prefix = 'ELASTICSEARCH_'


class LoggingConfig(BaseSettings):
    """
    Конфигурация логирования
    """
    level: str = 'INFO'
    format: str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    class Config:
        env_prefix = 'LOGGING_'


# Контекст для подключения к Postgres
@contextmanager
def pg_connection(connection_params: dict):
    conn = psycopg2.connect(**connection_params)
    try:
        yield conn
    finally:
        conn.close()


def chunks(lst, n):
    # Функция для разделения списка на пачки определенного размера
    for i in range(0, len(lst), n):
        yield lst[i:i + n]
