from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import logging
from utils import backoff


class ElasticsearchLoader:
    """
    Класс для загрузки данных в Elasticsearch
    """
    def __init__(self, es_host: str) -> None:
        self.es = Elasticsearch(es_host)
        self.logger = logging.getLogger('elasticsearch_loader')

    @backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10)
    def bulk_load(self, index: str, data: list) -> int:
        """
        Загрузка данных в Elasticsearch
        """
        actions = [
            {
                '_index': index,
                '_id': movie_data['id'],
                '_source': movie_data
            }
            for movie_data in data
        ]

        try:
            success, _ = bulk(self.es, actions, stats_only=True)
            self.logger.info(f'Successfully indexed {success} documents')
            return success
        except Exception as e:
            self.logger.error(f'Error during bulk index: {e}')
            raise
