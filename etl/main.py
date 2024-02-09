from load import ElasticsearchLoader
from extract import PostgresProducer, PostgresInricher, PostgresMerger
from transform import transform_film_work_details
import logging
from configs import PostgresConfig, LoggingConfig, ElasticsearchConfig
from state_manager import State, JsonFileStorage
from dotenv import load_dotenv

# Загрузка конфигурации
load_dotenv()
postgres_config = PostgresConfig().dict()
logging_config = LoggingConfig()
elasticsearch_config = ElasticsearchConfig()

# Настройка логирования
logging.basicConfig(level=getattr(logging, logging_config.level), format=logging_config.format)
logger = logging.getLogger(__name__)


# Основной код ETL процесса
def update_films(producer, merger, es_loader):
    """
    Обновление данных о фильмах
    """
    updated_film_work_ids = producer.fetch_updated_film_work_ids()
    if not updated_film_work_ids:
        logger.info('No film updates found.')
        return

    film_work_details = merger.fetch_film_work_details([film['id'] for film in updated_film_work_ids])
    transformed_data = transform_film_work_details(film_work_details)

    try:
        es_loader.bulk_load("movies", transformed_data)
        logger.info(f'Successfully loaded {len(transformed_data)} films to Elasticsearch.')
    except Exception as e:
        logger.error(f'Failed to load film data into Elasticsearch: {e}')
        raise

    return max(film['updated_at'] for film in updated_film_work_ids)


def update_persons(producer, inricher, merger, es_loader):
    """
    Обновление данных о персонах
    """
    updated_person_ids = producer.fetch_updated_person_ids()
    if not updated_person_ids:
        logger.info('No person updates found.')
        return

    related_film_works = inricher.fetch_related_film_works([person['id'] for person in updated_person_ids])
    film_work_details = merger.fetch_film_work_details([fw['id'] for fw in related_film_works])

    transformed_data = transform_film_work_details(film_work_details)

    try:
        es_loader.bulk_load("movies", transformed_data)
        logger.info(f'Successfully loaded related films to Elasticsearch.')
    except Exception as e:
        logger.error(f'Failed to load person-related film data into Elasticsearch: {e}')
        raise

    return max(person['updated_at'] for person in updated_person_ids)


def main():
    """
    Основной код ETL процесса
    """

    # Инициализация менеджера состояний
    state_manager = State(JsonFileStorage('etl/etl_state.json'))

    # Инициализация ETL процесса
    producer = PostgresProducer(postgres_config, state_manager)
    inricher = PostgresInricher(postgres_config)
    merger = PostgresMerger(postgres_config)

    # Запуск ETL процесса
    es_loader = ElasticsearchLoader(elasticsearch_config.host)

    try:
        last_film_update = update_films(producer, merger, es_loader)
        if last_film_update:
            state_manager.set_state('last_film_update', last_film_update.isoformat())

        last_person_update = update_persons(producer, inricher, merger, es_loader)
        if last_person_update:
            state_manager.set_state('last_person_update', last_person_update.isoformat())

    except Exception as e:
        logger.error(f'An error occurred during the ETL process: {e}')


if __name__ == "__main__":
    main()
