DEBUG = True

import os
BASE_DIR = os.path.abspath(os.path.dirname(__file__))  

# Database
SQLALCHEMY_DATABASE_URI = 'postgresql://' + os.environ.get('PSQL_USER') + ':' + os.environ.get('PSQL_PASSWORD') + '@localhost:5432/ironkip'
DATABASE_CONNECT_OPTIONS = {}

THREADS_PER_PAGE = 2

# Enable protection agains *Cross-site Request Forgery (CSRF)*
CSRF_ENABLED     = True 
CSRF_SESSION_KEY = os.environ.get('SESSION_KEY')

# Secret key for signing cookies
SECRET_KEY = os.environ.get('SECRET_KEY')