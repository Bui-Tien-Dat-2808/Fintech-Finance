import os
from flask_appbuilder.const import AUTH_DB

basedir = os.path.abspath(os.path.dirname(__file__))

# Flask-WTF flag for CSRF
WTF_CSRF_ENABLED = False
WTF_CSRF_TIME_LIMIT = None

# Isolate session cookie to prevent collision with other localhost apps
SESSION_COOKIE_NAME = "airflow_session"

AUTH_TYPE = AUTH_DB

