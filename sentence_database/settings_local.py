SECRET_KEY = '!^)af#1^da$1h6ov4kxp#c0za%j5c2@ot5mykb8q4&2+o8@23l'

RQ_QUEUES = {
    'default': {
        'HOST': 'localhost',
        'PORT': 6379,
        'DB': 0,
        'DEFAULT_TIMEOUT': 1800,
    }
}
