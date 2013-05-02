"""
Serve up search results via REST requests

Provide JSON results including the ID and the search snippet for given search
requests.

Ultimately needs to support advanced search as well, including NOT operators
and wildcards.
"""
import json
import psycopg2
import flask
app = flask.Flask(__name__)

DB_NAME = 'newspappy'
DB_HOST = 'localhost'
DB_USER = 'denials'

DB = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER)

@app.route("/search/<query>/")
@app.route("/search/<query>/<int:limit>")
@app.route("/search/<query>/<int:limit>/<int:page>")
def search(query, limit=10, page=0):
    """Simple search for terms, with optional limit and paging"""
    sql = """
        WITH q AS (SELECT plainto_tsquery(%s) AS query),
        ranked AS (
            SELECT id, ocr, ts_rank_cd(tsv, query) AS rank
            FROM items, q WHERE q.query @@ tsv
            ORDER BY rank DESC
            LIMIT %s OFFSET %s
        )
        SELECT id, ts_headline(ocr, q.query)
        FROM ranked, q
        ORDER BY ranked DESC
    """
    cur = DB.cursor()
    cur.execute(sql, (query, limit, page*limit))
    res = cur.fetchall()
    resj = json.dumps({'query': query, 'results': res})
    response = flask.Response(response="%s" % resj, mimetype='application/json')
    return response

if __name__ == "__main__":
    app.debug = True
    app.run(port=6000)
