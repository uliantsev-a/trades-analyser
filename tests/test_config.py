from flask import Flask
from unittest import TestCase
from flask_fixtures import FixturesMixin
from project import db, config


class BaseTestCase(TestCase, FixturesMixin):
    db = db

    @staticmethod
    def create_app():
        test_app = Flask(__name__)
        test_app.config.from_object(config.TestingConfig)

        from project.api.routes import mod_api  # noqa: E402
        from project.site.routes import mod_site  # noqa: E402

        test_app.register_blueprint(mod_api, url_prefix='/api')
        test_app.register_blueprint(mod_site)

        return test_app

    @classmethod
    def setUpClass(cls):
        cls.app = cls.create_app()
        cls.db.init_app(cls.app)
        cls.db.create_all()

    @classmethod
    def tearDownClass(cls):
        cls.db.drop_all()
        super().tearDownClass()

    def tearDown(self):
        self.db.session.rollback()
        self.db.drop_all()
        super().tearDown()
