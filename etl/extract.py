from contextlib import contextmanager
from datetime import datetime

import psycopg2
import logging

from configs import PostgresConfig, LoggingConfig, pg_connection
from state_manager import State, JsonFileStorage
from dotenv import load_dotenv
from psycopg2.extras import DictCursor

# Загрузка конфигурации
load_dotenv()
postgres_config = PostgresConfig()
logging_config = LoggingConfig()

# Настройка логирования
logging.basicConfig(level=getattr(logging, logging_config.level), format=logging_config.format)
logger = logging.getLogger(__name__)


class PostgresProducer:
    """
    Класс для извлечения данных из Postgres
    """

    def __init__(self, connection_params: dict, state_manager: State):
        # Параметры подключения к Postgres
        self.connection_params = postgres_config.dict()
        self.state_manager = state_manager

    def fetch_updated_ids(self):
        # Получение идентификаторов обновленных записей
        last_processed_time = self.state_manager.get_state('last_processed_time')
        query = """
        SELECT id, updated_at
        FROM content.person
        WHERE updated_at > %s
        ORDER BY updated_at
        LIMIT 100;
        """
        try:
            with pg_connection(self.connection_params) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, (last_processed_time,))
                    records = cursor.fetchall()
                    logger.info(f"Fetched {len(records)} records")
                    return records
        except psycopg2.Error as e:
            logger.error("Error fetching updated IDs from Postgres", exc_info=True)
            return []

    def update_state(self, last_processed_time):
        # Преобразование объекта datetime в строку формата ISO
        last_processed_time_str = last_processed_time.isoformat() if last_processed_time else None

        # Обновление состояния
        self.state_manager.set_state('last_processed_time', last_processed_time_str)

        # Метод для извлечения данных о фильмах

    def fetch_movies(self, last_update):
        # Метод для извлечения данных о фильмах
        query = """
        SELECT fw.id, fw.title, fw.description, fw.rating, fw.type,
               array_agg(DISTINCT g.name) as genres
        FROM content.film_work fw
        LEFT JOIN content.genre_film_work gfw ON fw.id = gfw.film_work_id
        LEFT JOIN content.genre g ON g.id = gfw.genre_id
        WHERE fw.updated_at > %s
        GROUP BY fw.id;
        """
        return self._fetch_data(query, last_update)

    # Метод для извлечения данных о персонах
    def fetch_persons(self, last_update):
        query = """
        SELECT p.id, p.full_name, pfw.role, pfw.film_work_id
        FROM content.person p
        INNER JOIN content.person_film_work pfw ON p.id = pfw.person_id
        WHERE p.updated_at > %s;
        """
        return self._fetch_data(query, last_update)

    # Метод для извлечения данных о жанрах
    def fetch_genres(self, last_update):
        query = """
        SELECT g.id, g.name, gfw.film_work_id
        FROM content.genre g
        INNER JOIN content.genre_film_work gfw ON g.id = gfw.genre_id
        WHERE g.updated_at > %s;
        """
        return self._fetch_data(query, last_update)

        # Общий метод для выполнения запросов

    def _fetch_data(self, query, last_update):
        # Общий метод для выполнения запросов
        with psycopg2.connect(**self.connection_params) as conn:
            with conn.cursor(cursor_factory=DictCursor) as cursor:
                cursor.execute(query, (last_update,))
                return cursor.fetchall()


# Путь к файлу состояния
state_file_path = 'etl/etl_state.json'

# Создание объекта для управления состоянием
state_manager = State(JsonFileStorage(state_file_path))

# Извлечение обновлённых ID и обновление состояния
# Основной код для запуска ETL процесса
if __name__ == "__main__":
    # Инициализация класса с параметрами подключения и менеджером состояния
    producer = PostgresProducer(postgres_config, state_manager)

    # Запрашиваем последнее обновление из состояния ETL
    last_update = state_manager.get_state('last_update') or datetime.min

    # Извлечение данных о фильмах, персонах и жанрах
    movies = producer.fetch_movies(last_update)
    persons = producer.fetch_persons(last_update)
    genres = producer.fetch_genres(last_update)
