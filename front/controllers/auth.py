# Import flask dependencies
from flask import Blueprint, request, render_template, \
                  flash, g, session, redirect, url_for
from werkzeug import check_password_hash, generate_password_hash
from front import db, login_manager, login_user, login_required, current_user, logout_user, logging
from front.forms.login import LoginForm, SignupForm
from front.models.user import User

mod_auth = Blueprint('auth', __name__)

@mod_auth.route('/signup', methods=['GET', 'POST'])
def signup():

    form = SignupForm(request.form)

    if form.validate_on_submit():
        new_user = User(form.first_name.data, form.last_name.data,
                        form.email.data,
                        generate_password_hash(form.password.data))

        try:
            db.session.add(new_user)
            db.session.commit()
            return redirect(request.args.get("next") or url_for("welcome.home"))
        except:
            db.session.rollback()

    return render_template("signup.html", form=form)

# Set the route and accepted methods
@mod_auth.route('/login', methods=['GET', 'POST'])
def login():

    # If sign in form is submitted
    form = LoginForm(request.form)

    logger.info(request.args.get('next', ''))

    # Verify the sign in form
    if form.validate_on_submit():

        user = User.query.filter_by(email=form.email.data).first()
        remember = request.form.get("remember", "no") == "yes"

        if user:
            if check_password_hash(user.password, form.password.data):
                user.authenticated = True
                db.session.add(user)
                db.session.commit()
                login_user(user, remember=True)
                return redirect(url_for('welcome.home'))
            else:
             	flash(u'Invalid email or password', 'warning');
        else:
            flash(u'Invalid email or password', 'warning')
            #return redirect(url_for('.login'))

    return render_template("login.html", form=form)


@mod_auth.route('/logout', methods=['GET', 'POST'])
@login_required
def logout():
    user = current_user
    user.is_authenticated = False
    db.session.add(user)
    db.session.commit()
    logout_user()
    return redirect(url_for('.login'))


@login_manager.user_loader
def load_user(uid):
    # 1. Fetch against the database a user by `id` 
    # 2. Create a new object of `User` class and return it.
    u = User.query.filter_by(id=uid).first()
    if u:
        return u
    else:
        return False
