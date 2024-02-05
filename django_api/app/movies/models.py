import uuid
from django.utils.translation import gettext_lazy as _
from django.core.validators import MinValueValidator, MaxValueValidator
from django.db import models


class TimeStampedMixin(models.Model):
    created_at = models.DateTimeField(_('created_at'), auto_now_add=True)
    updated_at = models.DateTimeField(_('updated_at'), auto_now=True)

    class Meta:
        abstract = True


class UUIDMixin(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    class Meta:
        abstract = True


class Genre(UUIDMixin, TimeStampedMixin):
    name = models.CharField(_('name'), max_length=255)
    description = models.TextField(_('description'),
                                   blank=True, null=True)

    class Meta:
        db_table = "content\".\"genre"
        verbose_name = _('Genre')
        verbose_name_plural = _('Genres')

    def __str__(self):
        return self.name


class Person(UUIDMixin, TimeStampedMixin):
    full_name = models.CharField(_('full_name'), max_length=255)

    class Meta:
        db_table = "content\".\"person"
        verbose_name = _('Person')
        verbose_name_plural = _('Persons')

    def __str__(self):
        return self.full_name


class FilmWorkType(models.TextChoices):
    MOVIE = 'movie', 'Фильм'
    TV_SHOW = 'tv_show', 'ТВ шоу'


class FilmWork(UUIDMixin, TimeStampedMixin):
    title = models.CharField(_('title'), max_length=255)

    description = models.TextField(_('description'),
                                   null=True,
                                   blank=True)

    creation_date = models.DateField(_('creation_date'),
                                     null=True, blank=True)

    rating = models.FloatField(_('rating'), blank=True,
                               null=True,
                               validators=[MinValueValidator(0),
                                           MaxValueValidator(100)])
    type = models.CharField(
        _('type'),
        max_length=10,
        choices=FilmWorkType.choices,
        default=FilmWorkType.MOVIE
    )
    genres = models.ManyToManyField(Genre, through='GenreFilmWork')
    persons = models.ManyToManyField(Person, through='PersonFilmWork')

    file_path = models.FileField(_('file'),
                                 blank=True, null=True,
                                 upload_to='movies/')

    class Meta:
        db_table = "content\".\"film_work"
        verbose_name = _('FilmWork')
        verbose_name_plural = _('FilmWorks')
        indexes = [
            models.Index(fields=['creation_date'], name='film_work_creation_date_idx'),
            models.Index(fields=['title'], name='film_work_title_idx'),
        ]

    def __str__(self):
        return self.title


class GenreFilmWork(UUIDMixin):
    film_work = models.ForeignKey(FilmWork, on_delete=models.CASCADE)
    genre = models.ForeignKey(Genre, on_delete=models.CASCADE)
    created_at = models.DateTimeField(_('created_at'), auto_now_add=True)

    class Meta:
        db_table = "content\".\"genre_film_work"
        indexes = [
            models.Index(fields=['genre', 'film_work'], name='genre_film_work_idx'),
        ]


class PersonRoleType(models.TextChoices):
    DIRECTOR = 'director', _('Director')
    WRITER = 'writer', _('Writer')
    ACTOR = 'actor', _('Actor')


class PersonFilmWork(UUIDMixin):
    film_work = models.ForeignKey(FilmWork, on_delete=models.CASCADE)
    person = models.ForeignKey(Person, on_delete=models.CASCADE)
    role = models.CharField(
        _('role'),
        max_length=255,
        choices=PersonRoleType.choices
    )
    created_at = models.DateTimeField(_('created_at'), auto_now_add=True)

    class Meta:
        db_table = "content\".\"person_film_work"
        constraints = [
            models.UniqueConstraint(fields=['film_work', 'person', 'role'],
                                    name='film_work_person_role_idx'),
        ]
