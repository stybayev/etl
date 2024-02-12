from pydantic import BaseModel, ValidationError
import logging
from typing import List, Dict, Any, Optional
from collections import defaultdict

logger = logging.getLogger('transform')


class Actor(BaseModel):
    id: str
    name: str


class Writer(BaseModel):
    id: str
    name: Optional[str]  # Разрешаем None для имени писателя


class FilmWork(BaseModel):
    id: str
    imdb_rating: Optional[float]
    genre: List[str]
    title: str
    description: Optional[str] = ''
    director: Optional[str]
    actors_names: List[str]
    writers_names: List[Optional[str]]
    actors: List[Actor]
    writers: List[Writer]


def transform_film_work_details(film_work_data: List) -> List[Dict[str, Any]]:
    """
    Преобразование данных о фильмах
    """
    transformed_data = []
    film_works = defaultdict(dict)

    for entry in film_work_data:
        fw_id = entry['fw_id']
        role = entry['role']
        person_id = str(entry['person_id'])
        full_name = entry['full_name']
        name = entry['name']

        # Инициализируем список актеров и режиссеров
        if fw_id not in film_works:
            film_works[fw_id] = {
                "id": fw_id,
                "imdb_rating": float(entry['rating'])
                if entry['rating'] is not None else None,

                "genre": [],
                "title": entry['title'],
                "description": entry['description']
                if entry['description'] is not None else '',

                "director": '',
                "actors_names": [],
                "writers_names": [],
                "actors": [],
                "writers": []
            }

        # Добавляем жанры к фильму
        if name and name not in film_works[fw_id]["genre"]:
            film_works[fw_id]["genre"].append(name)

        # Добавляем актеров и режиссеров к фильму
        if role == 'actor':
            if person_id not in [actor['id']
                                 for actor in film_works[fw_id]["actors"]]:
                film_works[fw_id]["actors"].append({"id": person_id,
                                                    "name": full_name})
                if full_name not in film_works[fw_id]["actors_names"]:
                    film_works[fw_id]["actors_names"].append(full_name)
        elif role == 'writer':
            if person_id not in [writer['id']
                                 for writer in film_works[fw_id]["writers"]]:
                film_works[fw_id]["writers"].append({"id": person_id,
                                                     "name": full_name})
                if full_name not in film_works[fw_id]["writers_names"]:
                    film_works[fw_id]["writers_names"].append(full_name)

        # Присвоение режиссера его фильму
        if entry['role'] == 'director':
            film_works[entry['fw_id']]['director'] = entry['full_name']

    # Валидация и преобразование
    for fw_id, film_work in film_works.items():
        try:

            if not film_work["director"]:
                film_work["director"] = ''
            else:
                # Если у нас есть список режиссеров, преобразуем его в строку
                film_work["director"] = film_work["director"]

            # Валидация данных с помощью Pydantic
            validated_data = FilmWork.parse_obj(film_work)

            # Добавление валидированных и преобразованных данных в список
            transformed_data.append(validated_data.dict())
        except ValidationError as e:
            # Логирование ошибок валидации
            logger.error(f"Data validation error "
                         f"for film work ID {fw_id}: {e.json()}")
            raise e
        except Exception as e:
            # Логирование непредвиденных ошибок
            logger.error(f"Unexpected error for film work ID {fw_id}: {e}")
            raise e

    return transformed_data
