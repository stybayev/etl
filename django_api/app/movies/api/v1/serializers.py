from rest_framework import serializers
from movies.models import FilmWork, Person, Genre

from movies.models import PersonRoleType


class FilmWorkSerializer(serializers.ModelSerializer):
    genres = serializers.SlugRelatedField(
        many=True,
        read_only=True,
        slug_field='id'
    )
    actors = serializers.SerializerMethodField()
    directors = serializers.SerializerMethodField()
    writers = serializers.SerializerMethodField()

    class Meta:
        model = FilmWork
        fields = '__all__'

    def get_actors(self, obj):
        # Возвращает список идентификаторов актеров для фильма
        return obj.persons.filter(personfilmwork__role=PersonRoleType.ACTOR.value).values_list('id', flat=True)

    def get_directors(self, obj):
        # Возвращает список идентификаторов режиссеров для фильма
        return obj.persons.filter(personfilmwork__role=PersonRoleType.DIRECTOR.value).values_list('id', flat=True)

    def get_writers(self, obj):
        # Возвращает список идентификаторов сценаристов для фильма
        return obj.persons.filter(personfilmwork__role=PersonRoleType.WRITER.value).values_list('id', flat=True)
