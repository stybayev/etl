CSRF_TRUSTED_ORIGINS = [
    'http://localhost:8000',
]

CORS_ALLOWED_ORIGINS = [
    'http://localhost:8000',
]

CORS_ORIGIN_WHITELIST = (
    'http://localhost:8000',
)

CSRF_COOKIE_SECURE = False

CORS_ALLOW_ALL_ORIGINS = True

CORS_ALLOW_CREDENTIALS = True

CORS_ALLOW_HEADERS = [
    'accept',
    'accept-encoding',
    'authorization',
    'content-type',
    'dnt',
    'origin',
    'user-agent',
    'x-csrftoken',
    'x-requested-with',
]
