import psycopg2
import logging

from configs import PostgresConfig
from state_manager import State, JsonFileStorage
from dotenv import load_dotenv

load_dotenv()
postgres_config = PostgresConfig()
# Настройка логирования
logging.basicConfig(level=logging.INFO)
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
            with psycopg2.connect(**self.connection_params) as conn:
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


# Путь к файлу состояния
state_file_path = 'etl/etl_state.json'

# Создание объекта для управления состоянием
state_manager = State(JsonFileStorage(state_file_path))

# Создание экземпляра PostgresProducer
producer = PostgresProducer(postgres_config, state_manager)

# Извлечение обновлённых ID и обновление состояния
if __name__ == "__main__":
    updated_ids = producer.fetch_updated_ids()
    if updated_ids:
        # Пример обновления состояния, используя последнее время обновления из выборки
        last_time = updated_ids[-1][1]
        producer.update_state(last_time)
