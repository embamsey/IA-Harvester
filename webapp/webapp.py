"""
Simple search UI for newspaper collections

Needs to offer:
* full-text search
* date limiters

"""
import json
import flask
import urllib, urllib2
from jinja2 import Environment, FileSystemLoader

env = Environment(loader=FileSystemLoader(searchpath='./templates'))
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
    jsonu = "http://localhost:6000/search/%s/" % (urllib.quote_plus(query))
    res = json.loads(urllib2.urlopen(jsonu).read())
    template = env.get_template('results.html')
    return(template.render(results=res['results']))

if __name__ == "__main__":
    app.debug = True
    app.run()
