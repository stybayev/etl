import time
from functools import wraps
import logging
import os
import json

logger = logging.getLogger(__name__)


def backoff(start_sleep_time: float = 0.1,
            factor: int = 2,
            border_sleep_time: int = 10) -> callable:
    """
    Функция для реализации backoff
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            sleep_time = start_sleep_time
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    logger.info(f'Ошибка: {e}. '
                                f'Повторное выполнение через '
                                f'{sleep_time} секунд...')
                    time.sleep(sleep_time)
                    sleep_time = min(sleep_time * factor, border_sleep_time)

        return wrapper

    return decorator


def create_etl_state_json(file_path: str):
    # Проверяем, существует ли файл
    if not os.path.isfile(file_path):

        # Создаем файл с начальными данными
        with open(file_path, 'w') as file:
            json.dump({
                'last_person_update': '2020-06-16T20:14:09.514476+00:00',
                'last_film_update': '2020-06-16T20:14:09.271850+00:00',
                'last_genre_update': '2020-06-16T20:14:09.310972+00:00'
            }, file)
        logger.info(f'Файл {file_path} был создан')
    else:
        logger.info(f'Файл {file_path} уже существует')
