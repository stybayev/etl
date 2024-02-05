from rest_framework import generics
from movies.models import FilmWork
from .serializers import FilmWorkSerializer

from .services.pagination import CustomPageNumberPagination


class FilmWorkList(generics.ListAPIView):
    """
    Получение списка фильмов
    """
    queryset = FilmWork.objects.prefetch_related('genres', 'persons').all()
    serializer_class = FilmWorkSerializer
    pagination_class = CustomPageNumberPagination


class FilmWorkDetail(generics.RetrieveAPIView):
    """
    Получение детальной информации о фильме
    """
    queryset = FilmWork.objects.prefetch_related('genres', 'persons').all()
    serializer_class = FilmWorkSerializer
