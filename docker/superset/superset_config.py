import os

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "k1jh2g3nasbfciaadzc")
SQLALCHEMY_DATABASE_URI = os.environ.get(
    "SQLALCHEMY_DATABASE_URI",
    "postgresql+psycopg2://admin:admin@postgres:5432/superset",
)

# Isolate session cookie to prevent collision with Airflow on localhost
SESSION_COOKIE_NAME = "superset_session"

# Disable CSRF token validation to allow seamless login on local docker setup
WTF_CSRF_ENABLED = False
WTF_CSRF_TIME_LIMIT = None
TALISMAN_ENABLED = False
ENABLE_PROXY_FIX = True

