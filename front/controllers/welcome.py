from flask import Blueprint, request, render_template, \
	flash, g, session, redirect, url_for

# Import the database object from the main app module
from app import db, login_required

# Import module forms
#from app.mod_auth.forms import LoginForm

# Import module models (i.e. User)
from app.models.user import User
#from app.mod_auth.models import User

# Define the blueprint: 'auth', set its url prefix: app.url/auth
mod_welcome = Blueprint('welcome', __name__, url_prefix='/')

# Set the route and accepted methods
@mod_welcome.route('/', methods=['GET', 'POST'])
@login_required
def home():
    # print(session)
    return render_template("welcome.html")


@mod_welcome.route('/user/<user_id>')
def user(user_id):
    return render_template("welcome.html", user=user_id)
