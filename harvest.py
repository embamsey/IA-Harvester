#!/usr/bin/env python3
"""
Harvest Internet Archive metadata and full-text for a given colllection

When you upload content to an Internet Archive collection, such as scanned
newspapers, the Internet Archive will OCR the content (if possible) and
generate a thumbnail image.

Given that it seems to be difficult (or impossible) to restrict one's
full-text search to a given collection on the Internet Archive, another option
is to retrieve the metadata, thumbnail, and OCRed text to build a
collection-specific web app. This script currently uses this data to populate
a table named "items" in a PostgreSQL database. The table is set up to support
full-text search, and stores the thumbnails in a subdirectory.

To-do:
  * build the web app
  * improve tolerance for IA download / connection errors
  * learn to do iterative crawls to pick up metadata corrections / new items
"""

import datetime
import json
import os
import os.path
import postgresql
import re
import time
import urllib.parse
import urllib.request

datadir = './data'
collections = ['laurentianuniversitylambda']
db_name = 'newspappy'
db_host = 'localhost'
db_user = 'denials'

db = postgresql.open(host=db_host, database=db_name, user=db_user)

def get_fulltext(did, ocr):
    """Grab the OCRed text for a given Internet Archive collection item"""
    ia = "http://archive.org/download/%s/%s" % (did, ocr)

    fname = os.path.join(datadir, 'fulltext/', did + '.json')
    if os.access(fname, os.R_OK):
        ft = open(fname, "rb").read()
    else:
        try:
            ft = urllib.request.urlopen(ia).read()
            f = open(fname, "wb")
        except Exception as exc:
            print("ERR: Failed to get full-text for %s" % (did))
            return ''
        else:
            with f:
                f.write(ft)

    # Repair hyphenation at column boundaries
    ft = re.sub(r'-\s*$\n', '', ft.decode('utf-8'), flags=re.MULTILINE)
    return ft

def get_details(docid):
    """Grab the metadata for a given Internet Archive collection item"""
    deets = {'id': docid['identifier']}
    didu = "http://archive.org/details/%s?output=json" % (deets['id'])
    dts = bytearray()

    fname = os.path.join(datadir, 'details/', deets['id'] + '.json')
    if os.access(fname, os.R_OK):
        dts = open(fname, "rb").read()
    else:
        time.sleep(1)
        try:
            dts = urllib.request.urlopen(didu).read()
            f = open(fname, "wb")
        except Exception as e:
            print("ERR: Could not fetch metadata for %s" % deets['id'])
            return None
        else:
            with f:
                f.write(dts)

    dts_res = json.loads(dts.decode('utf-8'), parse_int=True)

    # grab the following details for each :
    # image, title, date, year
    deets['image'] = dts_res['misc']['image']
    deets['title'] = dts_res['metadata']['title'][0]

    if 'year' in dts_res['metadata']:
        deets['year'] = int(dts_res['metadata']['year'][0])
    else:
        print("ERR: %s is missing year metadata" % (deets['id']))
        deets['year'] = 1900

    if 'date' in dts_res['metadata']:
        raw_date = dts_res['metadata']['date'][0]
    else:
        raw_date = str(deets['year'])

    try:
        deets['date'] = datetime.datetime.strptime(raw_date, '%Y-%m-%d')
    except ValueError as e:
        print("ERR: %s %s" % (deets['id'], e))
        try:
            deets['date'] = datetime.datetime.strptime(raw_date, '%Y-%m')
        except ValueError as e:
            deets['date'] = datetime.datetime.strptime(raw_date, '%Y')

    for f in dts_res['files']:
        if dts_res['files'][f]['format'] != 'DjVuTXT':
            continue
        ft = get_fulltext(deets['id'], f)
        deets['text'] = ft
    return deets

def load_db(c, d):
    """Add the details to the database"""

    if 'text' not in d:
        print("ERR: No text found for %s" % (d['id']))
        return

    ins = db.prepare("""
        INSERT INTO items(id, collection, title, image, date, year, ocr)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
    """)
    r = ins(d['id'], c, d['title'], d['image'], d['date'], d['year'], d['text'])

def init_db():
    """Initialize our database"""
    db.execute("DROP TABLE IF EXISTS items")
    db.execute("CREATE TABLE items (id TEXT, collection TEXT, title TEXT, image TEXT, date DATE, year INT, ocr TEXT, tsv TSVECTOR)")
    db.execute("CREATE INDEX tsv_idx ON items USING GIN(tsv)")
    db.execute("""
        CREATE TRIGGER tsv_update BEFORE INSERT OR UPDATE ON items
        FOR EACH ROW EXECUTE PROCEDURE
        tsvector_update_trigger(tsv, 'pg_catalog.english', ocr)
    """)

def get_image(d):
    """Grab the thumbnail for the Internet Archive item"""

    if 'image' not in d:
        print("ERR: No image found for %s" % d['id'])
        d['image'] = ''
        return

    print(d['image'])
    fname = os.path.join(datadir, 'images/', d['id'] + '.gif')
    if os.access(fname, os.R_OK):
        return
    try:
        r = urllib.request.urlopen(d['image']).read()
        f = open(fname, "wb")
    except Exception as e:
        print("ERR: Could not fetch image %s" % (d['image']))
    else:
        with f:
            f.write(r)

def get_collection(c):
    """Identify all of the items in a given Internet Archive collection"""
    page = 1
    rows = 100
    while True:
        res = get_page(c, page, rows)
        if int(res['response']['numFound']) < (rows * page):
            break
        page += 1

def get_page(c, page, rows):
    """Get one page of items from a given Internet Archive collection"""
    params = urllib.parse.urlencode({
        'q': "collection:%s" % (c),
        'fl[]': 'identifier',
        'sort[]': 'date asc',
        'rows': rows,
        'page': page,
        'output': 'json'
    })
    ids = urllib.request.urlopen("http://archive.org/advancedsearch.php?%s" % params)
    res = json.loads(ids.read().decode('utf-8'))
    for docid in res['response']['docs']:
        d = get_details(docid)
        if not d:
            next
        get_image(d)
        load_db(c, d)
    return res

os.makedirs(datadir, exist_ok=True)
os.makedirs(os.path.join(datadir, 'details'), exist_ok=True)
os.makedirs(os.path.join(datadir, 'fulltext'), exist_ok=True)
os.makedirs(os.path.join(datadir, 'images'), exist_ok=True)
init_db()
for c in collections:
    get_collection(c)

