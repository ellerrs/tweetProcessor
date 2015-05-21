from flask import Flask
from flask import render_template


def create_app(configfile=None):
    app = Flask(__name__)

    @app.route('/')
    def index():
        return render_template('hello.html')

    return app

if __name__ == '__main__':
    create_app().run(debug=True)
