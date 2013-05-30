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

DATADIR = './data'
COLLECTIONS = ['laurentianuniversitylambda', 'laurentianuniversitylorignaldechainereaction']
DB_NAME = 'newspappy'
DB_HOST = 'localhost'
DB_USER = 'postgres'

DB = postgresql.open(host=DB_HOST, database=DB_NAME, user=DB_USER)

def get_fulltext(did, ocr):
    """Grab the OCRed text for a given Internet Archive collection item"""
    ocr_url = "http://archive.org/download/%s/%s" % (did, ocr)

    fname = os.path.join(DATADIR, 'fulltext/', did + '.json')
    if os.access(fname, os.R_OK):
        contents = open(fname, "rb").read()
    else:
        try:
            contents = urllib.request.urlopen(ocr_url).read()
            out = open(fname, "wb")
        except Exception as exc:
            print("ERR: Failed to get full-text for %s : %s" % (did, exc))
            return ''
        else:
            with out:
                out.write(contents)

    # Repair hyphenation at column boundaries
    try:
        contents = contents.decode('utf-8')
        contents = re.sub(r'-\s*$\n', '', contents, flags=re.MULTILINE)
    except Exception as exc:
        print("WARN: Failed to decode full-text of %s as UTF8: %s" % (did, exc))
        contents = str(contents)

    return contents

def get_metadata(docid):
    """Grab the metadata for a given Internet Archive collection item"""
    deets = {'id': docid}
    didu = "http://archive.org/details/%s?output=json" % (docid)
    dts = bytearray()

    fname = os.path.join(DATADIR, 'details/', docid + '.json')
    if os.access(fname, os.R_OK):
        dts = open(fname, "rb").read()
    else:
        time.sleep(1)
        try:
            dts = urllib.request.urlopen(didu).read()
            out = open(fname, "wb")
        except Exception as exc:
            print("ERR: Could not fetch metadata for %s : %s" % (docid, exc))
            return None
        else:
            with out:
                out.write(dts)

    dts_res = json.loads(dts.decode('utf-8'), parse_int=True)

    # grab the following metadata for each :
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
    except ValueError as exc:
        print("ERR: %s %s" % (deets['id'], exc))
        try:
            deets['date'] = datetime.datetime.strptime(raw_date, '%Y-%m')
        except ValueError:
            deets['date'] = datetime.datetime.strptime(raw_date, '%Y')

    for fname in dts_res['files']:
        if dts_res['files'][fname]['format'] != 'DjVuTXT':
            continue
        deets['text'] = get_fulltext(deets['id'], fname)
    return deets

def load_db(collection, metadata):
    """Add the metadata to the database"""

    if 'text' not in metadata:
        print("ERR: No text found for %s" % (metadata['id']))
        return

    ins = DB.prepare("""
        INSERT INTO items(id, collection, title, image, date, year, ocr)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
    """)
    ins(
        metadata['id'],
        collection,
        metadata['title'],
        metadata['image'],
        metadata['date'],
        metadata['year'],
        metadata['text']
    )

def init_db():
    """Initialize our database"""
    DB.execute("DROP TABLE IF EXISTS items")
    DB.execute("""
        CREATE TABLE items (id TEXT, collection TEXT, title TEXT,
        image TEXT, date DATE, year INT, ocr TEXT, tsv TSVECTOR)
    """)
    DB.execute("CREATE INDEX tsv_idx ON items USING GIN(tsv)")
    DB.execute("""
        CREATE TRIGGER tsv_update BEFORE INSERT OR UPDATE ON items
        FOR EACH ROW EXECUTE PROCEDURE
        tsvector_update_trigger(tsv, 'pg_catalog.english', ocr)
    """)

def get_image(metadata):
    """Grab the thumbnail for the Internet Archive item"""

    if 'image' not in metadata:
        print("ERR: No image found for %s" % metadata['id'])
        metadata['image'] = ''
        return

    print(metadata['image'])
    fname = os.path.join(DATADIR, 'images/', metadata['id'] + '.gif')
    if os.access(fname, os.R_OK):
        return
    try:
        contents = urllib.request.urlopen(metadata['image']).read()
        out = open(fname, "wb")
    except Exception as exc:
        print("ERR: Could not fetch image %s : %s" % (metadata['image'], exc))
    else:
        with out:
            out.write(contents)

def get_collection(collection):
    """Identify all of the items in a given Internet Archive collection"""
    page = 1
    rows = 100
    while True:
        res = get_page(collection, page, rows)
        if int(res['response']['numFound']) < (rows * page):
            break
        page += 1

def get_page(collection, page, rows):
    """Get one page of items from a given Internet Archive collection"""
    params = urllib.parse.urlencode({
        'q': "collection:%s" % (collection),
        'fl[]': 'identifier',
        'sort[]': 'date asc',
        'rows': rows,
        'page': page,
        'output': 'json'
    })
    ids = urllib.request.urlopen(
        "http://archive.org/advancedsearch.php?%s" % params
    )
    res = json.loads(ids.read().decode('utf-8'))
    for docid in res['response']['docs']:
        metadata = get_metadata(docid['identifier'])
        if not metadata:
            continue
        get_image(metadata)
        load_db(collection, metadata)
    return res

if __name__ == "__main__":
    os.makedirs(DATADIR, exist_ok=True)
    os.makedirs(os.path.join(DATADIR, 'details'), exist_ok=True)
    os.makedirs(os.path.join(DATADIR, 'fulltext'), exist_ok=True)
    os.makedirs(os.path.join(DATADIR, 'images'), exist_ok=True)
    init_db()
    for c in COLLECTIONS:
        get_collection(c)

