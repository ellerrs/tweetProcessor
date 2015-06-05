# Import Form and RecaptchaField (optional)
from flask.ext.wtf import Form  # , RecaptchaField

# Import Form elements such as TextField and BooleanField (optional)
from wtforms import TextField, PasswordField, BooleanField

# Import Form validators
from wtforms.validators import Required, Email, EqualTo

# Define the login form (WTForms)


class LoginForm(Form):
    email = TextField('Email Address', [Email(), Required(
        message='Forgot your email address?')])
    password = PasswordField(
        'Password', [Required(message='You must provide a password.')])
    remember_me = BooleanField('Remember Me?')


class SignupForm(Form):
    first_name = TextField(
        'First Name', [Required(message='Please tell us your first name.')])
    last_name = TextField('Last Name',
                          [Required(message='Please tell us your last name.')])
    email = TextField('Email Address', [Email(), Required(
        message='Forgot your email address?')])
    password = PasswordField(
        'Password', [Required(message='You must provide a password.')])
