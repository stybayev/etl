import os
import django

django.setup()
from django.core.asgi import get_asgi_application  # noqa
from channels.routing import ProtocolTypeRouter, URLRouter  # noqa
from events.routing import websocket_urlpatterns  # noqa

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')

application = ProtocolTypeRouter({
    "http": get_asgi_application(),
    "websocket": URLRouter(websocket_urlpatterns)
})
