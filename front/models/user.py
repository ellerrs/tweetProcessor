from front import db
from front.models.base import Base


class User(Base):

    __tablename__ = 'users'

    # User Name
    first_name = db.Column(db.String(128), nullable=False)
    last_name = db.Column(db.String(128), nullable=False)

    # Identification Data: email & password
    email = db.Column(db.String(128), nullable=False, unique=True)
    password = db.Column(db.String(192), nullable=False)

    # Authorisation Data: role & status
    role = db.Column(db.SmallInteger, nullable=False)
    status = db.Column(db.SmallInteger, nullable=False)

    # Flask-Login integration
    def is_authenticated(self):
        return True

    def is_active(self):
        return True

    def is_anonymous(self):
        return False

    def get_id(self):
        return self.id

    # Required for administrative interface
    def __unicode__(self):
        return self.email

    # New instance instantiation procedure
    def __init__(self, first_name, last_name, email, password):

        self.first_name = first_name
        self.last_name = last_name
        self.email = email
        self.password = password
        self.role = 0
        self.status = 0

    def __repr__(self):
        return '<User %r %r>' % (self.first_name, self.last_name)
