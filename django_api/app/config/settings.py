import os
from pathlib import Path
from dotenv import load_dotenv
from split_settings.tools import include

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent

ROOT_URLCONF = 'config.urls'

WSGI_APPLICATION = 'config.wsgi.application'

include(
    'components/base.py',
    'components/security.py',
    'components/development.py',
    'components/applications.py',
    'components/middleware.py',
    'components/templates.py',
    'components/database.py',
    'components/static_media.py',
    'components/internationalization.py',
    'components/api_drf.py',
    'components/cors.py',
    scope=globals()
)
