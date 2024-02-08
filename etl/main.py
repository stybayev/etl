from extract import PostgresProducer, PostgresInricher, PostgresMerger
from transform import transform_film_work_details
from datetime import datetime
import logging
from configs import PostgresConfig, LoggingConfig
from state_manager import State, JsonFileStorage
from dotenv import load_dotenv

# Загрузка конфигурации
load_dotenv()
postgres_config = PostgresConfig().dict()
logging_config = LoggingConfig()

# Настройка логирования
logging.basicConfig(level=getattr(logging, logging_config.level), format=logging_config.format)
logger = logging.getLogger(__name__)


# Основной код ETL процесса
def main():
    # Создание объекта для управления состоянием
    state_manager = State(JsonFileStorage('etl/etl_state.json'))

    # Инициализация классов с параметрами подключения
    producer = PostgresProducer(postgres_config, state_manager)
    inricher = PostgresInricher(postgres_config)
    merger = PostgresMerger(postgres_config)

    # Извлечение обновлённых ID персон
    updated_person_ids = producer.fetch_updated_person_ids()
    updated_person_ids_list = [person['id'] for person in updated_person_ids]

    # Извлечение связанных фильмов
    if updated_person_ids_list:
        related_film_works = inricher.fetch_related_film_works(updated_person_ids_list)
        related_film_works_list = [fw['id'] for fw in related_film_works]

        # Извлечение деталей о фильмах
        if related_film_works_list:
            film_work_details = merger.fetch_film_work_details(related_film_works_list)
            print(f'Film work details: {film_work_details}')

            # Преобразование данных

            transformed_data = transform_film_work_details(film_work_details)
            print(f'Transformed data: {transformed_data}')

            # Здесь должен быть код для загрузки данных в Elasticsearch

    # Обновление состояния
    state_manager.set_state('last_person_update', datetime.now().isoformat())


if __name__ == "__main__":
    main()
