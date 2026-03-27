import os

SECRET_KEY = os.environ.get('SUPERSET_SECRET_KEY', 'lakehouse_superset_secret_2024')

SQLALCHEMY_DATABASE_URI = (
    'postgresql+psycopg2://superset:superset123@postgres-superset:5432/superset'
)

SQLALCHEMY_EXAMPLES_URI = 'trino://trino@trino:8085/delta'

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

WTF_CSRF_ENABLED = False
TALISMAN_ENABLED = False
ENABLE_PROXY_FIX = True
SESSION_COOKIE_SECURE = False
SESSION_COOKIE_SAMESITE = 'Lax'

ROW_LIMIT = 5000
SQL_MAX_ROW = 100000
