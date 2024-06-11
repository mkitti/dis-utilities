''' dis_responder.py
    UI and REST API for Data and Information Services
'''

from datetime import datetime, timedelta
import inspect
from json import JSONEncoder, dumps
from operator import attrgetter
import re
import os
import sys
from time import time
import bson
from flask import (Flask, make_response, render_template, request, jsonify)
from flask_cors import CORS
from flask_swagger import swagger
import requests
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL

# pylint: disable=broad-exception-caught,too-many-lines

__version__ = "1.7.0"
# Database
DB = {}
# Navigation
NAV = {"Home": "",
       "Stats" : {"DOI type": "stats_type",
                  "DOI publisher": "stats_publisher",
                  "Database": "stats_database"
                },
       #"ORCID": {"Lookup": "orcid"
       #         }
      }

# ******************************************************************************
# * Classes                                                                    *
# ******************************************************************************

class CustomJSONEncoder(JSONEncoder):
    ''' Define a custom JSON encoder
    '''
    def default(self, o):
        try:
            if isinstance(o, bson.objectid.ObjectId):
                return str(o)
            if isinstance(o, datetime):
                return o.strftime("%a, %-d %b %Y %H:%M:%S")
            if isinstance(o, timedelta):
                seconds = o.total_seconds()
                hours = seconds // 3600
                minutes = (seconds % 3600) // 60
                seconds = seconds % 60
                return f"{hours:02d}:{minutes:02d}:{seconds:.02f}"
            iterable = iter(o)
        except TypeError:
            pass
        else:
            return list(iterable)
        return JSONEncoder.default(self, o)


class InvalidUsage(Exception):
    ''' Return an error response
    '''
    status_code = 400

    def __init__(self, message, status_code=None, payload=None):
        Exception.__init__(self)
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        ''' Build error response
        '''
        retval = dict(self.payload or ())
        retval['rest'] = {'status_code': self.status_code,
                          'error': True,
                          'error_text': self.message}
        return retval

app = Flask(__name__, template_folder="templates")
app.json_encoder = CustomJSONEncoder
app.config.from_pyfile("config.cfg")
CORS(app, supports_credentials=True)
app.json_encoder = CustomJSONEncoder
app.config["STARTDT"] = datetime.now()
app.config["LAST_TRANSACTION"] = time()


# ******************************************************************************
# * Flask                                                                      *
# ******************************************************************************

@app.before_request
def before_request():
    ''' Set transaction start time and increment counters.
        If needed, initilize global variables.
    '''
    if not DB:
        try:
            dbconfig = JRC.get_config("databases")
        except Exception as err:
            return render_template('warning.html', urlroot=request.url_root,
                                   title=render_warning("Config error"), message=err)
        dbo = attrgetter("dis.prod.read")(dbconfig)
        print(f"Connecting to {dbo.name} prod on {dbo.host} as {dbo.user}")
        try:
            DB['dis'] = JRC.connect_database(dbo)
        except Exception as err:
            return render_template('warning.html', urlroot=request.url_root,
                                   title=render_warning("Database connect error"), message=err)
    app.config["START_TIME"] = time()
    app.config["COUNTER"] += 1
    endpoint = request.endpoint if request.endpoint else "(Unknown)"
    app.config["ENDPOINTS"][endpoint] = app.config["ENDPOINTS"].get(endpoint, 0) + 1
    if request.method == "OPTIONS":
        result = initialize_result()
        return generate_response(result)
    return None


# ******************************************************************************
# * Utility functions                                                          *
# ******************************************************************************

@app.errorhandler(InvalidUsage)
def handle_invalid_usage(error):
    ''' Error handler
        Keyword arguments:
          error: error object
    '''
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response


def inspect_error(err, errtype):
    ''' Render an error with inspection
        Keyword arguments:
          err: exception
        Returns:
          Error template
    '''
    mess = f"{inspect.stack()[0][3]} An exception of type {type(err).__name__} occurred. " \
           + f"Arguments:\n{err.args}"
    return render_template('error.html', urlroot=request.url_root,
                           title=render_warning(errtype), message=mess)


def generate_navbar(active):
    ''' Generate the web navigation bar
        Keyword arguments:
          Navigation bar
    '''
    nav = '''
    <nav class="navbar navbar-expand-lg navbar-dark bg-dark">
      <div class="collapse navbar-collapse" id="navbarSupportedContent">
        <ul class="navbar-nav mr-auto">
    '''
    for heading, subhead in NAV.items():
        basic = '<li class="nav-item active">' if heading == active else '<li class="nav-item">'
        drop = '<li class="nav-item dropdown active">' if heading == active \
               else '<li class="nav-item dropdown">'
        menuhead = '<a class="nav-link dropdown-toggle" href="#" id="navbarDropdown" ' \
                   + 'role="button" data-toggle="dropdown" aria-haspopup="true" ' \
                   + f"aria-expanded=\"false\">{heading}</a><div class=\"dropdown-menu\" "\
                   + 'aria-labelledby="navbarDropdown">'
        if subhead:
            nav += drop + menuhead
            for itm, val in subhead.items():
                link = f"/{val}" if val else ('/' + itm.replace(" ", "_")).lower()
                nav += f"<a class='dropdown-item' href='{link}'>{itm}</a>"
            nav += '</div></li>'
        else:
            nav += basic
            link = ('/' + heading.replace(" ", "_")).lower()
            nav += f"<a class='nav-link' href='{link}'>{heading}</a></li>"
    nav += '</ul></div></nav>'
    return nav


def receive_payload():
    ''' Get a request payload (form or JSON).
        Keyword arguments:
          None
        Returns:
          payload dictionary
    '''
    pay = {}
    if not request.get_data():
        return pay
    try:
        if request.form:
            for itm in request.form:
                pay[itm] = request.form[itm]
        elif request.json:
            pay = request.json
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    return pay


def initialize_result():
    ''' Initialize the result dictionary
        An auth header with a JWT token is required for all POST and DELETE requests
        Returns:
          decoded partially populated result dictionary
    '''
    result = {"rest": {"requester": request.remote_addr,
                       "url": request.url,
                       "endpoint": request.endpoint,
                       "error": False,
                       "elapsed_time": "",
                       "row_count": 0,
                       "pid": os.getpid()}}
    if app.config["LAST_TRANSACTION"]:
        print(f"Seconds since last transaction: {time() - app.config['LAST_TRANSACTION']}")
    app.config["LAST_TRANSACTION"] = time()
    return result


def generate_response(result):
    ''' Generate a response to a request
        Keyword arguments:
          result: result dictionary
        Returns:
          JSON response
    '''
    result["rest"]["elapsed_time"] = str(timedelta(seconds=time() - app.config["START_TIME"]))
    return jsonify(**result)


def get_work_publication_date(wsumm):
    ''' Get a publication date from an ORCID work summary
        Keyword arguments:
          wsumm: ORCID work summary
        Returns:
          Publication date
    '''
    date = ''
    if 'publication-date' in wsumm and wsumm['publication-date']:
        ppd = wsumm['publication-date']
        if 'year' in ppd and ppd['year']['value']:
            date = ppd['year']['value']
        if 'month' in ppd and ppd['month'] and ppd['month']['value']:
            date += f"-{ppd['month']['value']}"
        if 'day' in ppd and ppd['day'] and ppd['day']['value']:
            date += f"-{ppd['day']['value']}"
    return date


def get_work_doi(work):
    ''' Get a DOI from an ORCID work
        Keyword arguments:
          work: ORCID work
        Returns:
          DOI
    '''
    if not work['external-ids']['external-id']:
        return ''
    for eid in work['external-ids']['external-id']:
        if eid['external-id-type'] != 'doi':
            continue
        if 'external-id-normalized' in eid:
            return eid['external-id-normalized']['value']
        if 'external-id-value' in eid:
            return eid['external-id-url']['value']
    return ''


def render_warning(msg, severity='error', size='lg'):
    ''' Render warning HTML
        Keyword arguments:
          msg: message
          severity: severity (warning, error, or success)
          size: glyph size
        Returns:
          HTML rendered warning
    '''
    icon = 'exclamation-triangle'
    color = 'goldenrod'
    if severity == 'error':
        color = 'red'
    elif severity == 'success':
        icon = 'check-circle'
        color = 'lime'
    elif severity == 'na':
        icon = 'minus-circle'
        color = 'gray'
    elif severity == 'missing':
        icon = 'minus-circle'
    elif severity == 'no':
        icon = 'times-circle'
        color = 'red'
    elif severity == 'warning':
        icon = 'exclamation-circle'
    return f"<span class='fas fa-{icon} fa-{size}' style='color:{color}'></span>" \
           + f"&nbsp;{msg}"


def humansize(num, suffix='B'):
    ''' Return a human-readable storage size
        Keyword arguments:
          num: size
          suffix: default suffix
        Returns:
          string
    '''
    for unit in ['', 'K', 'M', 'G', 'T']:
        if abs(num) < 1024.0:
            return f"{num:.1f}{unit}{suffix}"
        num /= 1024.0
    return "{num:.1f}P{suffix}"


def add_jrc_fields(row):
    ''' Add a table of custom JRC fields
        Keyword arguments:
          row: DOI record
        Returns:
          HTML
    '''
    jrc = {}
    prog = re.compile("^jrc_")
    for key, val in row.items():
        if not re.match(prog, key):
            continue
        if isinstance(val, list):
            val = ", ".join(val)
        jrc[key] = val
    if not jrc:
        return ""
    html = '<table class="standard">'
    for key in sorted(jrc):
        html += f"<tr><td>{key}</td><td>{jrc[key]}</td></tr>"
    html += "</table><br>"
    return html

# *****************************************************************************
# * Documentation                                                             *
# *****************************************************************************

@app.route('/doc')
def get_doc_json():
    ''' Show documentation
    '''
    try:
        swag = swagger(app)
    except Exception as err:
        return inspect_error(err, 'Could not parse swag')
    swag['info']['version'] = __version__
    swag['info']['title'] = "Data and Information Services"
    return jsonify(swag)


@app.route('/help')
def show_swagger():
    ''' Show Swagger docs
    '''
    return render_template('swagger_ui.html')

# *****************************************************************************
# * Admin endpoints                                                         *
# *****************************************************************************

@app.route("/stats")
def stats():
    '''
    Show stats
    Show uptime/requests statistics
    ---
    tags:
      - Diagnostics
    responses:
      200:
        description: Stats
      400:
        description: Stats could not be calculated
    '''
    tbt = time() - app.config['LAST_TRANSACTION']
    result = initialize_result()
    start = datetime.fromtimestamp(app.config['START_TIME']).strftime('%Y-%m-%d %H:%M:%S')
    up_time = datetime.now() - app.config['STARTDT']
    result['stats'] = {"version": __version__,
                       "requests": app.config['COUNTER'],
                       "start_time": start,
                       "uptime": str(up_time),
                       "python": sys.version,
                       "pid": os.getpid(),
                       "endpoint_counts": app.config['ENDPOINTS'],
                       "time_since_last_transaction": tbt,
                      }
    return generate_response(result)

# ******************************************************************************
# * API endpoints                                                              *
# ******************************************************************************
@app.route('/doi/<path:doi>')
def show_doi(doi):
    '''
    Return a DOI
    Return Crossref or DataCite information for a given DOI.
    If it's not in the dois collection, it will be retrieved from Crossref or Datacite.
    ---
    tags:
      - DOI
    parameters:
      - in: path
        name: doi
        schema:
          type: path
        required: true
        description: DOI
    responses:
      200:
        description: DOI data
      500:
        description: MongoDB error
    '''
    doi = doi.lstrip('/')
    doi = doi.rstrip('/')
    result = initialize_result()
    coll = DB['dis'].dois
    try:
        row = coll.find_one({"doi": doi}, {'_id': 0})
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    if row:
        result['rest']['row_count'] = 1
        result['rest']['source'] = 'mongo'
        result['data'] = row
        return generate_response(result)
    if DL.is_datacite(doi):
        resp = JRC.call_datacite(doi)
        result['rest']['source'] = 'datacite'
        result['data'] = resp['data'] if 'data' in resp else {}
    else:
        resp = JRC.call_crossref(doi)
        result['rest']['source'] = 'crossref'
        result['data'] = resp['message'] if 'message' in resp else {}
    if result['data']:
        result['rest']['row_count'] = 1
    return generate_response(result)


@app.route('/doi/inserted/<string:idate>')
def show_inserted(idate):
    '''
    Return DOIs inserted since a specified date
    Return all DOIs that have been inserted since midnight on a specified date.
    ---
    tags:
      - DOI
    parameters:
      - in: path
        name: idate
        schema:
          type: string
        required: true
        description: Earliest insertion date in ISO format (YYYY-MM-DD)
    responses:
      200:
        description: DOI data
      400:
        description: bad input data
      500:
        description: MongoDB error
    '''
    result = initialize_result()
    try:
        coll = DB['dis'].dois
        isodate = datetime.strptime(idate,'%Y-%m-%d')
    except Exception as err:
        raise InvalidUsage(str(err), 400) from err
    print(isodate)
    try:
        rows = coll.find({"jrc_inserted": {"$gte" : isodate}}, {'_id': 0})
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    result['rest']['row_count'] = 0
    result['rest']['source'] = 'mongo'
    result['data'] = []
    for row in rows:
        result['data'].append(row)
        result['rest']['row_count'] += 1
    return generate_response(result)


@app.route('/citation/<path:doi>')
@app.route('/citation/dis/<path:doi>')
def show_citation(doi):
    '''
    Return a DIS-style citation
    Return a DIS-style citation for a given DOI.
    ---
    tags:
      - DOI
    parameters:
      - in: path
        name: doi
        schema:
          type: path
        required: true
        description: DOI
    responses:
      200:
        description: DOI data
      404:
        description: DOI not found
      500:
        description: MongoDB or formatting error
    '''
    doi = doi.lstrip('/')
    doi = doi.rstrip('/')
    result = initialize_result()
    coll = DB['dis'].dois
    try:
        row = coll.find_one({"doi": doi}, {'_id': 0})
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    if not row:
        raise InvalidUsage(f"DOI {doi} is not in the database", 404)
    result['rest']['row_count'] = 1
    result['rest']['source'] = 'mongo'
    authors = DL.get_author_list(row)
    title = DL.get_title(row)
    result['data'] = f"{authors} {title}. https://doi.org/{doi}."
    return generate_response(result)


@app.route('/citations', defaults={'ctype': 'dis'}, methods=['OPTIONS', 'POST'])
@app.route('/citations/<string:ctype>', methods=['OPTIONS', 'POST'])
def show_multiple_citations(ctype='dis'):
    '''
    Return DIS-style citations
    Return a dictionary of DIS-style citations for a list of given DOIs.
    ---
    tags:
      - DOI
    parameters:
      - in: path
        name: ctype
        schema:
          type: string
        required: false
        description: Citation type (dis or flylight)
      - in: query
        name: dois
        schema:
          type: list
        required: true
        description: List of DOIs
    responses:
      200:
        description: DOI data
      500:
        description: MongoDB or formatting error
    '''
    result = initialize_result()
    ipd = receive_payload()
    if "dois" not in ipd or not (ipd['dois']) or not isinstance(ipd['dois'], list):
        raise InvalidUsage("You must specify a list of DOIs")
    result['rest']['source'] = 'mongo'
    result['data'] = {}
    coll = DB['dis'].dois
    for doi in ipd['dois']:
        try:
            row = coll.find_one({"doi": doi}, {'_id': 0})
        except Exception as err:
            raise InvalidUsage(str(err), 500) from err
        if not row:
            result['data'][doi] = ''
            continue
        result['rest']['row_count'] += 1
        authors = DL.get_author_list(row, style=ctype)
        title = DL.get_title(row)
        if ctype == 'dis':
            result['data'][doi] = f"{authors} {title}. https://doi.org/{doi}."
        else:
            journal = DL.get_journal(row)
            result['data'][doi] = f"{authors} {title}. {journal}."
    return generate_response(result)


@app.route('/citation/flylight/<path:doi>')
def show_flylight_citation(doi):
    '''
    Return a FlyLight-style citation
    Return a FlyLight-style citation for a given DOI.
    ---
    tags:
      - DOI
    parameters:
      - in: path
        name: doi
        schema:
          type: path
        required: true
        description: DOI
    responses:
      200:
        description: DOI data
      404:
        description: DOI not found
      500:
        description: MongoDB or formatting error
    '''
    doi = doi.lstrip('/')
    doi = doi.rstrip('/')
    result = initialize_result()
    coll = DB['dis'].dois
    try:
        row = coll.find_one({"doi": doi}, {'_id': 0})
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    if not row:
        raise InvalidUsage(f"DOI {doi} is not in the database", 404)
    result['rest']['row_count'] = 1
    result['rest']['source'] = 'mongo'
    authors = DL.get_author_list(row, style='flylight')
    title = DL.get_title(row)
    journal = DL.get_journal(row)
    result['data'] = f"{authors} {title}. {journal}."
    return generate_response(result)


@app.route('/components/<path:doi>')
def show_components(doi):
    '''
    Return components of a DIS-style citation
    Return components of a DIS-style citation for a given DOI.
    ---
    tags:
      - DOI
    parameters:
      - in: path
        name: doi
        schema:
          type: path
        required: true
        description: DOI
    responses:
      200:
        description: DOI data
      404:
        description: DOI not found
      500:
        description: MongoDB or formatting error
    '''
    doi = doi.lstrip('/')
    doi = doi.rstrip('/')
    result = initialize_result()
    coll = DB['dis'].dois
    try:
        row = coll.find_one({"doi": doi}, {'_id': 0})
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    if not row:
        raise InvalidUsage(f"DOI {doi} is not in the database", 404)
    result['rest']['row_count'] = 1
    result['rest']['source'] = 'mongo'
    result['data'] = {"authors": DL.get_author_list(row, returntype="list"),
                      "journal": DL.get_journal(row),
                      "publishing_date": DL.get_publishing_date(row),
                      "title": DL.get_title(row)
                     }
    if row['jrc_obtained_from'] == 'Crossref' and 'abstract' in row:
        result['data']['abstract'] = row['abstract']
    return generate_response(result)


@app.route('/orcid')
def show_oids():
    '''
    Show saved ORCID IDs
    Return information for saved ORCID IDs
    ---
    tags:
      - ORCID
    responses:
      200:
        description: ORCID data
      500:
        description: MongoDB error
    '''
    result = initialize_result()
    try:
        coll = DB['dis'].orcid
        rows = coll.find({}, {'_id': 0}).collation({"locale": "en"}).sort("family", 1)
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    result['rest']['source'] = 'mongo'
    result['data'] = []
    for row in rows:
        result['data'].append(row)
    result['rest']['row_count'] = len(result['data'])
    return generate_response(result)


@app.route('/orcid/<string:oid>')
def show_oid(oid):
    '''
    Show an ORCID ID
    Return information for an ORCID ID or name
    ---
    tags:
      - ORCID
    parameters:
      - in: path
        name: oid
        schema:
          type: string
        required: true
        description: ORCID ID, given name, or family name
    responses:
      200:
        description: ORCID data
      500:
        description: MongoDB error
    '''
    result = initialize_result()
    if re.match(r'([0-9A-Z]{4}-){3}[0-9A-Z]+', oid):
        payload = {"orcid": oid}
    else:
        payload = {"$or": [{"family": {"$regex": oid, "$options" : "i"}},
                           {"given": {"$regex": oid, "$options" : "i"}}]
                  }
    try:
        coll = DB['dis'].orcid
        rows = coll.find(payload, {'_id': 0})
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    result['rest']['source'] = 'mongo'
    result['data'] = []
    for row in rows:
        result['data'].append(row)
    return generate_response(result)


@app.route('/orcidapi/<string:oid>')
def show_oidapi(oid):
    '''
    Show an ORCID ID (using the ORCID API)
    Return information for an ORCID ID (using the ORCID API)
    ---
    tags:
      - ORCID
    parameters:
      - in: path
        name: oid
        schema:
          type: string
        required: true
        description: ORCID ID
    responses:
      200:
        description: ORCID data
    '''
    result = initialize_result()
    url = f"https://pub.orcid.org/v3.0/{oid}"
    try:
        resp = requests.get(url, headers={"Accept": "application/json"}, timeout=10)
        result['data'] = resp.json()
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    if 'error-code' not in result['data']:
        result['rest']['source'] = 'orcid'
        result['rest']['row_count'] = 1
    return generate_response(result)


@app.route('/groups')
def show_groups():
    '''
    Show groups from ORCID
    Return records whose IDs are in a group
    ---
    tags:
      - Groups
    responses:
      200:
        description: groups
      500:
        description: MongoDB error
    '''
    result = initialize_result()
    coll = DB['dis'].orcid
    payload = {"group": {"$exists": True}}
    try:
        rows = coll.find(payload, {'_id': 0}).sort("group", 1)
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    result['rest']['source'] = 'mongo'
    result['data'] = []
    for row in rows:
        result['data'].append(row)
    result['rest']['row_count'] = len(result['data'])
    return generate_response(result)


@app.route('/components', defaults={'ctype': 'dis'}, methods=['OPTIONS', 'POST'])
@app.route('/components/<string:ctype>', methods=['OPTIONS', 'POST'])
def show_multiple_components(ctype='dis'):
    '''
    Return components for a given group tag
    Return a list of citation components for a given group tag.
    ---
    tags:
      - Tags
    parameters:
      - in: path
        name: ctype
        schema:
          type: string
        required: false
        description: Citation type (dis or flylight)
      - in: query
        name: group
        schema:
          type: string
        required: true
        description: Group tag
    responses:
      200:
        description: Component data
      500:
        description: MongoDB or formatting error
    '''
    result = initialize_result()
    ipd = receive_payload()
    if "group" not in ipd or not (ipd['group']) or not isinstance(ipd['group'], str):
        raise InvalidUsage("You must specify a group")
    result['rest']['source'] = 'mongo'
    result['data'] = []
    coll = DB['dis'].dois
    try:
        rows = coll.find({"jrc_tag": ipd['group']}, {'_id': 0})
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    if not rows:
        generate_response(result)
    for row in rows:
        record = {"doi": row['doi'],
                  "authors": DL.get_author_list(row, style=ctype, returntype="list"),
                  "title": DL.get_title(row),
                  "journal": DL.get_journal(row),
                  "publishing_date": DL.get_publishing_date(row)
                 }
        if row['jrc_obtained_from'] == 'Crossref' and 'abstract' in row:
            record['abstract'] = row['abstract']
        result['data'].append(record)
        result['rest']['row_count'] += 1
    return generate_response(result)


@app.route('/tags', methods=['OPTIONS', 'POST'])
def show_multiple_tag_dois():
    '''
    Return DOIs for a given group tag
    Return a list of DOI records for a given group tag.
    ---
    tags:
      - Tags
    parameters:
      - in: query
        name: group
        schema:
          type: string
        required: true
        description: Group tag
    responses:
      200:
        description: DOI data
      500:
        description: MongoDB or formatting error
    '''
    result = initialize_result()
    ipd = receive_payload()
    if "group" not in ipd or not (ipd['group']) or not isinstance(ipd['group'], str):
        raise InvalidUsage("You must specify a group")
    result['rest']['source'] = 'mongo'
    result['data'] = []
    coll = DB['dis'].dois
    try:
        rows = coll.find({"jrc_tag": ipd['group']}, {'_id': 0})
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    if not rows:
        generate_response(result)
    for row in rows:
        result['data'].append(row)
        result['rest']['row_count'] += 1
    return generate_response(result)


@app.route('/types')
def show_types():
    '''
    Show data types
    Return data types, subtypes, and counts
    ---
    tags:
      - Types
    responses:
      200:
        description: types
      500:
        description: MongoDB error
    '''
    result = initialize_result()
    coll = DB['dis'].dois
    payload = [{"$group": {"_id": {"type": "$type", "subtype": "$subtype"},"count": {"$sum": 1}}}]
    try:
        rows = coll.aggregate(payload)
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    result['rest']['source'] = 'mongo'
    result['data'] = {}
    for row in rows:
        if 'type' not in row['_id']:
            result['data']['datacite'] = {"count": row['count'], "subtype": None}
        else:
            typ = row['_id']['type']
            result['data'][typ] = {"count": row['count']}
            result['data'][typ]['subtype'] = row['_id']['subtype'] if 'subtype' in row['_id'] \
                                             else None
    result['rest']['row_count'] = len(result['data'])
    return generate_response(result)

# ******************************************************************************
# * Web endpoints                                                              *
# ******************************************************************************
@app.route('/')
@app.route('/home')
def show_home():
    ''' Home
    '''
    response = make_response(render_template('home.html', urlroot=request.url_root,
                                             navbar=generate_navbar('Home')))
    return response


# ******************************************************************************
# * DOI endpoints                                                              *
# ******************************************************************************
@app.route('/doiui/<path:doi>')
def show_doi_ui(doi):
    ''' Show DOI
    '''
    doi = doi.lstrip('/')
    doi = doi.rstrip('/')
    coll = DB['dis'].dois
    try:
        row = coll.find_one({"doi": doi})
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    if row:
        html = '<h5 style="color:lime">This DOI is saved locally in the Janelia database</h5>'
        html += add_jrc_fields(row)
    else:
        html = '<h5 style="color:red">This DOI is not saved locally in the ' \
               + 'Janelia database</h5><br>'
    if DL.is_datacite(doi):
        resp = JRC.call_datacite(doi)
        data = resp['data'] if 'data' in resp else {}
    else:
        resp = JRC.call_crossref(doi)
        data = resp['message'] if 'message' in resp else {}
    if not data:
        return render_template('warning.html', urlroot=request.url_root,
                                title=render_warning("Could not find DOI", 'warning'),
                                message=f"Could not find DOI {doi}")
    authorlist = DL.get_author_list(data, orcid=True)
    if not authorlist:
        return render_template('error.html', urlroot=request.url_root,
                                title=render_warning("Could not generate author list"),
                                message=f"Could not generate author list for {doi}")
    title = DL.get_title(data)
    if not title:
        return render_template('error.html', urlroot=request.url_root,
                                title=render_warning("Could not find title"),
                                message=f"Could not find title for {doi}")
    citation = f"{authorlist} {title}."
    journal = DL.get_journal(data)
    if not journal:
        return render_template('error.html', urlroot=request.url_root,
                                title=render_warning("Could not find journal"),
                                message=f"Could not find journal for {doi}")
    outjson = dumps(data, indent=2).replace("\n", "<br>").replace(" ", "&nbsp;")
    link = f"https://dx.doi.org/{doi}"
    html += "<h4>Citation</h4>" + f"<span class='citation'>{citation} {journal}." \
            + f"<br><br>DOI: <a href='{link}' target='_blank'>{doi}</a></span>" \
            + f"<br><br><h4>Raw JSON</h4><div class='scroll'>{outjson}</div>"
    response = make_response(render_template('general.html', urlroot=request.url_root,
                                             title=doi, html=html,
                                             navbar=generate_navbar('DOIs')))
    return response


# ******************************************************************************
# * ORCID endpoints                                                            *
# ******************************************************************************
@app.route('/orcidui/<string:oid>')
def show_oid_ui(oid):
    ''' Show ORCID user
    '''
    try:
        resp = requests.get(f"https://pub.orcid.org/v3.0/{oid}",
                            headers={"Accept": "application/json"}, timeout=10)
        data = resp.json()
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                                title=render_warning("Could not retrieve ORCID ID"),
                                message=str(err))
    if 'person' not in data:
        return render_template('warning.html', urlroot=request.url_root,
                                title=render_warning(f"Could not find ORCID ID {oid}", 'warning'),
                                message=data['user-message'])
    name = data['person']['name']
    if name['credit-name']:
        who = f"{name['credit-name']['value']}"
    else:
        who = f"{name['given-names']['value']} {name['family-name']['value']}"
    coll = DB['dis'].orcid
    try:
        row = coll.find_one({"orcid": oid})
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    if row and 'userIdO365' in row:
        who = "<a href='" + f"{app.config['WORKDAY']}{row['userIdO365']}" \
              + f"' target='_blank'>{who}</a>"
    html = f"<h2>{who}</h2>"
    # Works
    if 'works' in data['activities-summary'] and data['activities-summary']['works']['group']:
        html += 'Note that titles below may be self-reported, and may not have DOIs available</br>'
        html += '<table id="ops" class="tablesorter standard"><thead><tr>' \
                + '<th>Published</th><th>DOI</th><th>Title</th>' \
                + '</tr></thead><tbody>'
        for work in data['activities-summary']['works']['group']:
            wsumm = work['work-summary'][0]
            date = get_work_publication_date(wsumm)
            doi = get_work_doi(work)
            if not doi:
                html += f"<tr><td>{date}</td><td>&nbsp;</td>" \
                        + f"<td>{wsumm['title']['title']['value']}</td></tr>"
                continue
            if work['external-ids']['external-id'][0]['external-id-url']:
                if work['external-ids']['external-id'][0]['external-id-url']:
                    link = "<a href='" \
                           + work['external-ids']['external-id'][0]['external-id-url']['value'] \
                           + f"' target='_blank'>{doi}</a>"
            else:
                link = f"<a href='https://dx.doi.org/{doi}' target='_blank'>{doi}</a>"
            html += f"<tr><td>{date}</td><td>{link}</td>" \
                    + f"<td>{wsumm['title']['title']['value']}</td></tr>"
        html += '</tbody></table>'
    response = make_response(render_template('general.html', urlroot=request.url_root,
                                             title=f"<a href='https://orcid.org/{oid}' " \
                                                   + f"target='_blank'>{oid}</a>", html=html,
                                             navbar=generate_navbar('ORCID')))
    return response


@app.route('/namesui/<string:name>')
def show_names_ui(name):
    ''' Show user names
    '''
    payload = {"$or": [{"family": {"$regex": name, "$options" : "i"}},
                       {"given": {"$regex": name, "$options" : "i"}},
                      ]}
    try:
        coll = DB['dis'].orcid
        if not coll.count_documents(payload):
            return render_template('warning.html', urlroot=request.url_root,
                                   title=render_warning("Could not find name", 'warning'),
                                    message=f"Could not find any name matching {name}")
        rows = coll.find(payload).collation({"locale": "en"}).sort("family", 1)
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    html = '<table id="ops" class="tablesorter standard"><thead><tr>' \
           + '<th>ORCID</th><th>Given name</th><th>Family name</th>' \
           + '</tr></thead><tbody>'
    for row in rows:
        link = f"<a href='/orcidui/{row['orcid']}'>{row['orcid']}</a>"
        html += f"<tr><td>{link}</td><td>{', '.join(row['given'])}</td>" \
                + f"<td>{', '.join(row['family'])}</td></tr>"
    html += '</tbody></table>'
    response = make_response(render_template('general.html', urlroot=request.url_root,
                                             title=f"Search term: {name}", html=html,
                                             navbar=generate_navbar('ORCID')))
    return response


# ******************************************************************************
# * Stat endpoints                                                             *
# ******************************************************************************
@app.route('/stats_type')
def stats_type():
    ''' Show data types
    '''
    payload = [{"$group": {"_id": {"source": "$jrc_obtained_from", "type": "$type",
                                   "subtype": "$subtype"},
                           "count": {"$sum": 1}}},
               {"$sort" : {"count": -1}}]
    try:
        coll = DB['dis'].dois
        rows = coll.aggregate(payload)
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    html = '<table id="types" class="tablesorter standard"><thead><tr>' \
           + '<th>Source</th><th>Type</th><th>Subtype</th><th>Count</th>' \
           + '</tr></thead><tbody>'
    for row in rows:
        for field in ('type', 'subtype'):
            if field not in row['_id']:
                row['_id'][field] = ''
        html += f"<tr><td>{row['_id']['source']}</td><td>{row['_id']['type']}</td>" \
                + f"<td>{row['_id']['subtype']}</td><td>{row['count']:,}</td></tr>"
    html += '</tbody></table>'
    response = make_response(render_template('general.html', urlroot=request.url_root,
                                             title="DOI types", html=html,
                                             navbar=generate_navbar('Stats')))
    return response


@app.route('/stats_publisher')
def stats_publisher():
    ''' Show publishers
    '''
    payload = [{"$group": {"_id": {"publisher": "$publisher"},
                           "count": {"$sum": 1}}},
               {"$sort" : {"count": -1}}]
    try:
        coll = DB['dis'].dois
        rows = coll.aggregate(payload)
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    html = '<table id="types" class="tablesorter standard"><thead><tr>' \
           + '<th>Publisher</th><th>Count</th>' \
           + '</tr></thead><tbody>'
    for row in rows:
        html += f"<tr><td>{row['_id']['publisher']}</td><td>{row['count']:,}</td></tr>"
    html += '</tbody></table>'
    response = make_response(render_template('general.html', urlroot=request.url_root,
                                             title="DOI publishers", html=html,
                                             navbar=generate_navbar('Stats')))
    return response


@app.route('/stats_database')
def stats_database():
    ''' Show database stats
    '''
    collection = {}
    try:
        cnames = DB['dis'].list_collection_names()
        for cname in cnames:
            stat = DB['dis'].command('collStats', cname)
            indices = []
            for key, val in stat['indexSizes'].items():
                indices.append(f"{key} ({humansize(val)})")
            free = stat['freeStorageSize'] / stat['storageSize'] * 100
            collection[cname] = {"docs": stat['count'],
                                 "size": humansize(stat['size']),
                                 "free": f"{free:.2f}",
                                 "idx": ", ".join(indices)
                                }
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    html = '<table id="collections" class="tablesorter standard"><thead><tr>' \
           + '<th>Collection</th><th>Documents</th><th>Size</th><th>Free space</th>' \
           + '<th>Indices</th></tr></thead><tbody>'
    for coll, val in sorted(collection.items()):
        html += f"<tr><td>{coll}</td><td>{val['docs']:,}</td><td>{val['size']}</td>" \
                + f"<td>{val['free']}%</td><td>{val['idx']}</td></tr>"
    html += '</tbody></table>'
    response = make_response(render_template('general.html', urlroot=request.url_root,
                                             title="Database statistics", html=html,
                                             navbar=generate_navbar('Stats')))
    return response

# *****************************************************************************

if __name__ == '__main__':
    if app.config["RUN_MODE"] == 'dev':
        app.run(debug=app.config["DEBUG"])
    else:
        app.run(debug=app.config["DEBUG"])
