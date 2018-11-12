from flask import Blueprint, render_template

mod_site = Blueprint('site', __name__, template_folder='templates')


# @mod_site.route('/')
@mod_site.route('/', defaults={'path': ''})
@mod_site.route('/<path:path>')
def homepage(path):
    """
    Get main page
    @return: index.html
    """
    return render_template('site/index.html')
