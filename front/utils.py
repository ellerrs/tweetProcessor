from functools import wraps
from datetime import datetime

from flask import flash, redirect, url_for

from flask.ext.login import current_user


def check_expired(func):

    @wraps(func)
    def decorated_function(*args, **kwargs):
        if datetime.utcnow() > current_user.account_expires:
            flash("Your account has expired. Update your billing info.")
            return redirect(url_for('account_billing'))
        return func(*args, **kwargs)

    return decorated_function
