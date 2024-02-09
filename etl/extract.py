from datetime import datetime
import psycopg2
from psycopg2.extras import DictCursor
from typing import List

from state_manager import State
from utils import backoff


class PostgresBase:
    """
    Базовый класс для работы с Postgres
    """

    def __init__(self, connection_params: dict) -> None:
        self.connection_params = connection_params

    @backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10)
    def _fetch_data(self, query, params) -> list:
        with psycopg2.connect(**self.connection_params) as conn:
            with conn.cursor(cursor_factory=DictCursor) as cursor:
                cursor.execute(query, params)
                return cursor.fetchall()


class PostgresInricher(PostgresBase):
    """
    Класс для получения подробной информации о фильмах и персонах
    """

    def fetch_related_film_works(self, person_ids: List[str]) -> list:
        """
        Получение списка фильмов, в которых участвует персона
        """

        # Преобразование строк в UUID
        str_person_ids = [str(id) for id in person_ids]

        query = """
                SELECT DISTINCT fw.id, fw.updated_at
                FROM content.film_work fw
                JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                WHERE pfw.person_id = ANY(%s::uuid[])
                ORDER BY fw.updated_at
                LIMIT 100;
                """
        return self._fetch_data(query, (str_person_ids,))

    def fetch_related_film_works_by_genre(self, genre_ids: List[str]) -> list:
        """
        Получение списка фильмов, связанных с определёнными жанрами.
        """
        # Преобразование строк в UUID
        str_genre_ids = [str(id) for id in genre_ids]

        query = """
                SELECT DISTINCT fw.id, fw.updated_at
                FROM content.film_work fw
                JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
                WHERE gfw.genre_id = ANY(%s::uuid[])
                ORDER BY fw.updated_at
                LIMIT 100;
                """
        return self._fetch_data(query, (str_genre_ids,))


class PostgresMerger(PostgresBase):
    """
    Класс для получения подробной информации о фильмах
    """

    def fetch_film_work_details(self, film_work_ids: List[str]) -> list:
        """
        Получение подробной информации о фильме
        """
        # Преобразование UUID в строки для корректной работы с запросом
        str_film_work_ids = [str(id) for id in film_work_ids]

        query = """
        SELECT
            fw.id as fw_id, 
            fw.title, 
            fw.description, 
            fw.rating, 
            fw.type, 
            fw.created_at, 
            fw.updated_at, 
            pfw.role, 
            p.id as person_id, 
            p.full_name,
            g.name
        FROM content.film_work fw
        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
        LEFT JOIN content.person p ON p.id = pfw.person_id
        LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
        LEFT JOIN content.genre g ON g.id = gfw.genre_id
        WHERE fw.id = ANY(%s::uuid[]);
       
        """
        return self._fetch_data(query, (str_film_work_ids,))


class PostgresProducer(PostgresBase):
    """
    Класс для получения обновленных ID
    """

    def __init__(self, connection_params: dict,
                 state_manager: State) -> None:
        super().__init__(connection_params)
        self.state_manager = state_manager

    def fetch_updated_film_work_ids(self) -> list:
        """
        Получение обновленных ID фильмов.
        """
        last_film_update = self.state_manager.get_state('last_film_update') or datetime.min
        query = """
        SELECT id, updated_at
        FROM content.film_work
        WHERE updated_at > %s
        ORDER BY updated_at
        LIMIT 100;
        """
        return self._fetch_data(query, (last_film_update,))

    def fetch_updated_person_ids(self) -> list:
        """
        Получение обновленных ID персон
        """
        last_person_update = self.state_manager.get_state('last_person_update') or datetime.min
        query = """
        SELECT id, updated_at
        FROM content.person
        WHERE updated_at > %s
        ORDER BY updated_at
        LIMIT 100;
        """
        return self._fetch_data(query, (last_person_update,))

    def fetch_updated_genres(self) -> list:
        """
        Получение обновленных данных о жанрах.
        """
        last_genre_update = self.state_manager.get_state('last_genre_update') or datetime.min
        query = """
        SELECT id, updated_at
        FROM content.genre
        WHERE updated_at > %s
        ORDER BY updated_at
        LIMIT 100;
        """
        return self._fetch_data(query, (last_genre_update,))
