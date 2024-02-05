from django.urls import path

from movies.api.v1 import views

urlpatterns = [
    path('movies/', views.FilmWorkList.as_view()),
    path('movies/<uuid:pk>/', views.FilmWorkDetail.as_view()),
]

