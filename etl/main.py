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


def update_genres_in_batches(producer, inricher, merger, es_loader, state_manager, batch_size=100):
    """
    Обновление данных о жанрах и связанных с ними фильмах пакетами
    """
    updated_genre_ids = producer.fetch_updated_genres()
    if not updated_genre_ids:
        logger.info('No genre updates found.')
        return

    for batch_start in range(0, len(updated_genre_ids), batch_size):
        batch_end = batch_start + batch_size
        genre_id_batch = updated_genre_ids[batch_start:batch_end]

        related_film_works = inricher.fetch_related_film_works_by_genre([genre['id'] for genre in genre_id_batch])
        if not related_film_works:
            logger.info(f'No film works related to genres in batch {batch_start}-{batch_end} found.')
            continue

        film_work_details = merger.fetch_film_work_details([fw['id'] for fw in related_film_works])
        transformed_data = transform_film_work_details(film_work_details)

        try:
            es_loader.bulk_load("movies", transformed_data)
            logger.info(
                f'Successfully loaded {len(transformed_data)} films for genres batch {batch_start}-{batch_end} into Elasticsearch.')
        except Exception as e:
            logger.error(f'Failed to load film data for genres batch {batch_start}-{batch_end} into Elasticsearch: {e}')
            raise

        max_updated_at = max(genre['updated_at'] for genre in genre_id_batch)
        state_manager.set_state(f'last_genre_update_batch_{batch_start}-{batch_end}', max_updated_at.isoformat())


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
        # Обновление данных о фильмах
        last_film_update = update_films(producer, merger, es_loader)
        if last_film_update:
            state_manager.set_state('last_film_update', last_film_update.isoformat())

        # Обновление данных о персонах
        last_person_update = update_persons(producer, inricher, merger, es_loader)
        if last_person_update:
            state_manager.set_state('last_person_update', last_person_update.isoformat())

        # Обновление данных о жанрах пакетами
        batch_size = 100

        update_genres_in_batches(producer, inricher, merger, es_loader, state_manager, batch_size)
        # Обратите внимание, что состояние для последнего обновления жанра устанавливается внутри функции update_genres_in_batches

    except Exception as e:
        logger.error(f'Произошла ошибка во время ETL процесса: {e}')
        # Здесь может быть код для обработки ошибок, например, повторное соединение или оповещение


if __name__ == "__main__":
    main()
