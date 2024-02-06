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
            scheme="postgres",
            user=self.user,
            password=self.password,
            host=self.host,
            port=str(self.port),
            path=f"/{self.dbname}"
        )
