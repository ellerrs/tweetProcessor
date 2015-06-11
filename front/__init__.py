from flask import Flask, render_template, logging
from flask.ext.sqlalchemy import SQLAlchemy
from flask.ext.login import (LoginManager, current_user, login_required,
                             login_user, logout_user, UserMixin,
                             AnonymousUserMixin, confirm_login,
                             fresh_login_required)

#from .utils import check_expired

front = Flask(__name__)
front.config.from_object('config')

from front import utils

login_manager = LoginManager()
login_manager.login_view = "auth.login"
login_manager.login_message = u"Please log in to access this page."
login_manager.refresh_view = "reauth"
login_manager.init_app(front)

# Define the database object which is imported
# by modules and controllers
db = SQLAlchemy(front)

# Sample HTTP error handling
@front.errorhandler(404)
def not_found(error):
    return render_template('404.html'), 404


from front.controllers.welcome import mod_welcome
from front.controllers.auth import mod_auth

front.register_blueprint(mod_welcome)
front.register_blueprint(mod_auth)

db.create_all()
