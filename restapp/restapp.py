"""
Serve up search results via REST requests

Provide JSON results including the ID and the search snippet for given search
requests.

Ultimately needs to support advanced search as well, including NOT operators
and wildcards.
"""

import config
import json
import psycopg2
import flask

app = flask.Flask(__name__)

DB = psycopg2.connect(
    host=config.DB_HOST,
    database=config.DB_NAME,
    user=config.DB_USER
)

@app.route("/search/<query>/")
@app.route("/search/<query>/<int:limit>")
@app.route("/search/<query>/<int:limit>/<int:page>")
def search(query, limit=10, page=0):
    """Return JSON formatted search results, including snippets and facets"""

    results = __get_ranked_results(query, limit, page)
    years = __get_year_facet(query)
    collections = __get_collection_facet(query)

    resj = json.dumps({
        'query': query,
        'results': results,
        'years': years,
        'collections': collections
    })
    response = flask.Response(response="%s" % resj, mimetype='application/json')
    return response

def __get_ranked_results(query, limit, page):
    """Simple search for terms, with optional limit and paging"""

    sql = """
        WITH q AS (SELECT plainto_tsquery(%s) AS query),
        ranked AS (
            SELECT id, collection, title, date, year, ocr, ts_rank(tsv, query) AS rank
            FROM items, q WHERE q.query @@ tsv
            ORDER BY rank DESC
            LIMIT %s OFFSET %s
        )
        SELECT id, collection, title, date::text, year, ts_headline(ocr, q.query, 'MaxWords=75,MinWords=25,ShortWord=3,MaxFragments=3,FragmentDelimiter="||||"')
        FROM ranked, q
        ORDER BY ranked DESC
    """

    cur = DB.cursor()
    cur.execute(sql, (query, limit, page*limit))
    results = []
    for row in cur:
        results.append({
            'id': row[0],
            'collection': row[1],
            'title': row[2],
            'date': row[3],
            'year': row[4],
            'snippets': row[5].split('||||')
        })
    cur.close()

    return results


def __get_year_facet(query):
    """Gather counts, by year, of matching results"""
    sql = """
        SELECT year, COUNT(year)
        FROM items
        WHERE plainto_tsquery(%s) @@ tsv
        GROUP BY year
        ORDER BY 2 DESC
    """

    cur = DB.cursor()
    cur.execute(sql, (query,))
    years = []
    for row in cur:
        years.append([
            row[0],
            row[1]
        ])

    return years

def __get_collection_facet(query):
    """Gather counts, by collection, of matching results"""
    sql = """
        SELECT collection, COUNT(collection)
        FROM items
        WHERE plainto_tsquery(%s) @@ tsv
        GROUP BY collection
        ORDER BY 2 DESC
    """
    cur = DB.cursor()
    cur.execute(sql, (query,))
    collections = []
    for row in cur:
        collections.append([
            row[0],
            row[1]
        ])

    return collections

if __name__ == "__main__":
    app.debug = True
    app.run(port=config.PORT)
