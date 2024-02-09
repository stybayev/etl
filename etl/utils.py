import time
from functools import wraps
import logging

logger = logging.getLogger(__name__)


def backoff(start_sleep_time: int = 0.1,
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
                    logger.info(f"Ошибка: {e}. Повторное выполнение через {sleep_time} секунд...")
                    time.sleep(sleep_time)
                    sleep_time = min(sleep_time * factor, border_sleep_time)

        return wrapper

    return decorator
