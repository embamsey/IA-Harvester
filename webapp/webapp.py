"""
Simple search UI for newspaper collections

Needs to offer:
* full-text search
* date limiters

"""

import config
import json
import flask
import os
import urllib, urllib2
from jinja2 import Environment, FileSystemLoader

env = Environment(loader=FileSystemLoader(searchpath="%s/templates" % os.path.dirname((os.path.realpath(__file__)))))
app = flask.Flask(__name__)

@app.route("/")
def index():
    template = env.get_template('index.html')
    return(template.render())

@app.route("/search")
def search():
    """Simple search for terms, with optional limit and paging"""
    query = flask.request.args.get('query', '')
    year = flask.request.args.get('year', '')
    collection = flask.request.args.get('collection', '')
    page = flask.request.args.get('page', '')
    jsonu = "%s/search/%s/" % (config.JSON_HOST, urllib.quote_plus(query))
    if page:
        jsonu = "%s%d" % (jsonu, int(page))
    if collection:
        jsonu = "%s?collection=%s" % (jsonu, str(collection))
    if year:
        jsonu = "%s?year=%d" % (jsonu, int(year))
    res = json.loads(urllib2.urlopen(jsonu).read())
    template = env.get_template('results.html')

    return(template.render(
        terms=res['query'].replace('+', ' '),
        results=res,
        request=flask.request
    ))

if __name__ == "__main__":
    app.debug = True
    print(config.JSON_HOST)
    app.run()
