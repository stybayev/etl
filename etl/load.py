from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import logging


class ElasticsearchLoader:
    """
    Класс для загрузки данных в Elasticsearch
    """
    def __init__(self, es_host):
        self.es = Elasticsearch(es_host)
        self.logger = logging.getLogger('elasticsearch_loader')

    def bulk_load(self, index, data):
        """
        Загрузка данных в Elasticsearch
        """
        actions = [
            {
                "_index": index,
                "_id": movie_data['id'],
                "_source": movie_data
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
