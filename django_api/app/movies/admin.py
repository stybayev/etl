from django.contrib import admin
from .models import (Genre, FilmWork, GenreFilmWork,
                     Person, PersonFilmWork)


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    list_display = ('name', 'created_at', 'updated_at')
    search_fields = ('name', 'description')


@admin.register(Person)
class PersonAdmin(admin.ModelAdmin):
    list_display = ('full_name', 'created_at', 'updated_at')
    search_fields = ('full_name',)


class GenreFilmWorkInline(admin.TabularInline):
    model = GenreFilmWork
    autocomplete_fields = ('genre',)


class PersonFilmWorkInline(admin.TabularInline):
    model = PersonFilmWork
    autocomplete_fields = ('person',)


@admin.register(FilmWork)
class FilmWorkAdmin(admin.ModelAdmin):
    inlines = (GenreFilmWorkInline, PersonFilmWorkInline)

    list_display = ('title', 'type', 'creation_date',
                    'rating', 'get_genres', 'get_persons')

    list_filter = ('type',)

    search_fields = ('title', 'description', 'id')

    autocomplete_fields = ['persons', ]

    list_prefetch_related = ('genres', 'persons',)

    def get_queryset(self, request):
        queryset = (
            super()
            .get_queryset(request)
            .prefetch_related(*self.list_prefetch_related)
        )
        return queryset

    def get_genres(self, obj):
        return ', '.join([genre.name for genre in obj.genres.all()])

    def get_persons(self, obj):
        return ', '.join([person.full_name for person in obj.persons.all()])

    get_genres.short_description = 'Жанры фильма'
    get_persons.short_description = 'Персоны фильма'
