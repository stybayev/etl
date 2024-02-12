from load import ElasticsearchLoader
from extract import PostgresProducer, PostgresInricher, PostgresMerger
from transform import transform_film_work_details
import logging
from configs import PostgresConfig, LoggingConfig, ElasticsearchConfig
from state_manager import State, JsonFileStorage
from dotenv import load_dotenv
import time
from typing import Optional

# Загрузка конфигурации
load_dotenv()
postgres_config = PostgresConfig().dict()
logging_config = LoggingConfig()
elasticsearch_config = ElasticsearchConfig()

# Настройка логирования
logging.basicConfig(level=getattr(logging, logging_config.level),
                    format=logging_config.format)

logger = logging.getLogger(__name__)


# Основной код ETL процесса
def update_films(producer: PostgresProducer,
                 merger: PostgresMerger,
                 es_loader: ElasticsearchLoader) -> Optional[int]:
    """
    Обновление данных о фильмах
    """
    updated_film_work_ids = producer.fetch_updated_film_work_ids()
    if not updated_film_work_ids:
        logger.info('No film updates found.')
        return

    film_work_details = merger.fetch_film_work_details(
        [film['id'] for film in updated_film_work_ids])

    transformed_data = transform_film_work_details(film_work_details)

    try:
        es_loader.bulk_load('movies', transformed_data)
        logger.info(f'Successfully loaded {len(transformed_data)} '
                    f'films to Elasticsearch.')

    except Exception as e:
        logger.debug('Failed to load film data into Elasticsearch: %s', e)
        raise

    return max(film['updated_at'] for film in updated_film_work_ids)


def update_persons(producer, inricher, merger, es_loader) -> Optional[int]:
    """
    Обновление данных о персонах
    """
    updated_person_ids = producer.fetch_updated_person_ids()
    if not updated_person_ids:
        logger.info('No person updates found.')
        return

    related_film_works = inricher.fetch_related_film_works(
        [person['id'] for person in updated_person_ids])

    film_work_details = merger.fetch_film_work_details(
        [fw['id'] for fw in related_film_works])

    transformed_data = transform_film_work_details(film_work_details)

    try:
        es_loader.bulk_load('movies', transformed_data)
        logger.info('Successfully loaded related '
                    'films to Elasticsearch.')
    except Exception as e:
        logger.debug('Failed to load person-related film '
                     'data into Elasticsearch: %s', e)
        raise

    return max(person['updated_at'] for person in updated_person_ids)


def update_genres(producer: PostgresProducer,
                  inricher: PostgresInricher,
                  merger: PostgresMerger,
                  es_loader: ElasticsearchLoader) -> Optional[int]:
    """
    Обновление данных о жанрах и связанных с ними фильмах
    """
    updated_genre_ids = producer.fetch_updated_genres()
    if not updated_genre_ids:
        logger.info('No genre updates found.')
        return

    related_film_works = inricher.fetch_related_film_works_by_genre(
        [genre['id'] for genre in updated_genre_ids])

    film_work_details = merger.fetch_film_work_details(
        [fw['id'] for fw in related_film_works])

    transformed_data = transform_film_work_details(film_work_details)

    try:
        es_loader.bulk_load('movies', transformed_data)
        logger.info('Successfully loaded related '
                    'films for updated genres to Elasticsearch.')
    except Exception as e:
        logger.debug('Failed to load genre-related '
                     'film data into Elasticsearch: %s', e)
        raise

    return max(genre['updated_at'] for genre in updated_genre_ids)


def main() -> None:
    """
    Основной код ETL процесса
    """
    # Инициализация менеджера состояний
    state_manager = State(JsonFileStorage('etl_state.json'))

    # Инициализация ETL процесса
    producer = PostgresProducer(postgres_config, state_manager)
    inricher = PostgresInricher(postgres_config)
    merger = PostgresMerger(postgres_config)
    es_loader = ElasticsearchLoader(elasticsearch_config.host)

    while True:
        try:
            # Обновление данных о фильмах
            last_film_update = update_films(producer, merger, es_loader)
            if last_film_update:
                state_manager.set_state('last_film_update',
                                        last_film_update.isoformat())

            # Обновление данных о персонах
            last_person_update = update_persons(producer,
                                                inricher,
                                                merger,
                                                es_loader)
            if last_person_update:
                state_manager.set_state('last_person_update',
                                        last_person_update.isoformat())

            #  Обновление данных о жанрах пакетами
            last_genre_update = update_genres(producer,
                                              inricher,
                                              merger,
                                              es_loader)
            if last_genre_update:
                state_manager.set_state('last_genre_update',
                                        last_genre_update.isoformat())

        except Exception as e:
            logger.debug('Произошла ошибка во время ETL процесса: %s', e)

        # Пауза перед следующим циклом обновления
        logger.info('Ожидание следующего цикла обновления...')
        time.sleep(0.5)


if __name__ == '__main__':
    main()
