import os

SECRET_KEY = os.environ.get('SUPERSET_SECRET_KEY', 'lakehouse_superset_secret_2024')

POSTGRES_USER = os.environ.get('POSTGRES_SUPERSET_USER', 'superset')
POSTGRES_PASSWORD = os.environ.get('POSTGRES_SUPERSET_PASSWORD', 'superset123')
POSTGRES_HOST = os.environ.get('POSTGRES_SUPERSET_HOST', 'postgres-superset')
POSTGRES_PORT = os.environ.get('POSTGRES_SUPERSET_PORT', '5432')
POSTGRES_DB = os.environ.get('POSTGRES_SUPERSET_DB', 'superset')

SQLALCHEMY_DATABASE_URI = (
    f'postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}'
    f'@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}'
)

SQLALCHEMY_EXAMPLES_URI = 'trino://trino@trino:8080/delta'

FAB_ADD_SECURITY_API = True

FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
}

CACHE_CONFIG = {
    'CACHE_TYPE': 'SimpleCache',
    'CACHE_DEFAULT_TIMEOUT': 300,
}

# Security settings — toggled by SUPERSET_ENV environment variable
IS_DEV = os.environ.get('SUPERSET_ENV', 'development') == 'development'
WTF_CSRF_ENABLED = not IS_DEV
TALISMAN_ENABLED = not IS_DEV
ENABLE_PROXY_FIX = True
SESSION_COOKIE_SECURE = not IS_DEV
SESSION_COOKIE_SAMESITE = 'Lax'

ROW_LIMIT = 5000
SQL_MAX_ROW = 100000
