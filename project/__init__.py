import os
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from flask_marshmallow import Marshmallow
from flask_cors import CORS

app = Flask(__name__, instance_relative_config=True)
CORS(app)


config = {
    "development": "project.config.DevelopmentConfig",
    "testing": "project.config.TestingConfig",
    "default": "project.config.DevelopmentConfig"
}

config_name = os.getenv('FLASK_CONFIGURATION', 'default')

app.config.from_object(config[config_name])
app.config.from_pyfile('config.cfg', silent=True)

db = SQLAlchemy(app)
ma = Marshmallow(app)
migrate = Migrate(app, db)


from project.api.routes import mod_api  # noqa E402
from project.site.routes import mod_site  # noqa E402


app.register_blueprint(mod_api, url_prefix='/api')
app.register_blueprint(mod_site)
