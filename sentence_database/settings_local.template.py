SECRET_KEY = 'xxx'

RQ_QUEUES = {
    'default': {
        'HOST': 'localhost',
        'PORT': 6379,
        'DB': 0,
        'DEFAULT_TIMEOUT': 1800,
    }
}
ALLOWED_HOSTS = ['sdbdev.christian.id.au']
