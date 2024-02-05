from django.contrib import admin
from django.urls import include, path
from django.conf import settings
from django.conf.urls.static import static
from drf_yasg.views import get_schema_view
from rest_framework import permissions
from drf_yasg import openapi
import os

schema_view = get_schema_view(
    openapi.Info(
        title='Movies API',
        default_version='v1',
        description='List of movies',
        terms_of_service='https://www.google.com/policies/terms/',
        contact=openapi.Contact(email='contact@snippets.local'),
        license=openapi.License(name='BSD License'),
    ),
    url=os.environ.get('SWAGGER_URL'),
    public=True,
    permission_classes=[permissions.AllowAny]
)

urlpatterns = [
    path('admin/', admin.site.urls),
    path('api/', include('movies.api.urls')),
    path("__debug__/", include("debug_toolbar.urls")),
]

urlpatterns_swagger = [
    path('api/api.json/', schema_view.without_ui(cache_timeout=0), name='schema-json'),
    path('', schema_view.with_ui('swagger', cache_timeout=0), name='schema-swagger-ui'),
    path('redoc/', schema_view.with_ui('redoc', cache_timeout=0), name='schema-redoc')
]

if settings.DEBUG:
    urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
    urlpatterns += urlpatterns_swagger
