from flask import Flask, render_template, logging
from flask.ext.sqlalchemy import SQLAlchemy
from flask.ext.login import (LoginManager, current_user, login_required,
                             login_user, logout_user, UserMixin,
                             AnonymousUserMixin, confirm_login,
                             fresh_login_required)

#from .utils import check_expired

app = Flask(__name__)
app.config.from_object('config')

from app import utils

login_manager = LoginManager()
login_manager.login_view = "auth.login"
login_manager.login_message = u"Please log in to access this page."
login_manager.refresh_view = "reauth"
login_manager.init_app(app)

# Define the database object which is imported
# by modules and controllers
db = SQLAlchemy(app)

# Sample HTTP error handling
@app.errorhandler(404)
def not_found(error):
    return render_template('404.html'), 404


from app.controllers.welcome import mod_welcome
from app.controllers.auth import mod_auth

app.register_blueprint(mod_welcome)
app.register_blueprint(mod_auth)

db.create_all()
