''' dis_responder.py
    UI and REST API for Data and Information Services
'''

from datetime import datetime, timedelta
import inspect
from json import JSONEncoder
from operator import attrgetter
import os
import random
import re
import string
import sys
from time import time
import bson
from flask import (Flask, make_response, render_template, request, jsonify, send_file)
from flask_cors import CORS
from flask_swagger import swagger
import requests
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL

# pylint: disable=broad-exception-caught,too-many-lines

__version__ = "4.10.0"
# Database
DB = {}
# Navigation
NAV = {"Home": "",
       "DOIs": {"DOIs by type": "dois_type",
                "DOIs by publisher": "dois_publisher",
                "DOIs by tag": "dois_tag"
            },
       "ORCID": {"Groups": "groups",
                 "Affiliations": "orcid_tag",
                 "Entries": "orcid_entry"
                },
       "Stats" : {"Database": "stats_database"
                 },
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
    ''' Class to populate error return for JSON.
    '''
    def __init__(self, message, status_code=400, payload=None):
        Exception.__init__(self)
        self.message = message
        self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        ''' Build error response
        '''
        retval = dict(self.payload or ())
        retval['rest'] = {'status_code': self.status_code,
                          'error': True,
                          'error_text': f"{self.message}\n" \
                                        + f"An exception of type {type(self).__name__} occurred. " \
                                        + f"Arguments:\n{self.args}"}
        return retval


class CustomException(Exception):
    ''' Class to populate error return for HTML.
    '''
    def __init__(self,message, preface=""):
        super().__init__(message)
        self.original = type(message).__name__
        self.args = message.args
        cfunc = inspect.stack()[1][3]
        self.preface = f"In {cfunc}, {preface}" if preface else f"Error in {cfunc}."


# ******************************************************************************
# * Flask                                                                      *
# ******************************************************************************

app = Flask(__name__, template_folder="templates")
app.json_encoder = CustomJSONEncoder
app.config.from_pyfile("config.cfg")
CORS(app, supports_credentials=True)
app.json_encoder = CustomJSONEncoder
app.config["STARTDT"] = datetime.now()
app.config["LAST_TRANSACTION"] = time()


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
        dbo = attrgetter("dis.prod.write")(dbconfig)
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
# * Error utility functions                                                    *
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


def error_message(err):
    ''' Create an error message from an exception
        Keyword arguments:
          err: exception
        Returns:
          Error message
    '''
    if isinstance(err, CustomException):
        msg = f"{err.preface}\n" if err.preface else ""
        msg += f"An exception of type {err.original} occurred. Arguments:\n{err.args}"
    else:
        msg = f"An exception of type {type(err).__name__} occurred. Arguments:\n{err.args}"
    return msg


def inspect_error(err, errtype):
    ''' Render an error with inspection
        Keyword arguments:
          err: exception
        Returns:
          Error screen
    '''
    mess = f"In {inspect.stack()[1][3]}, An exception of type {type(err).__name__} occurred. " \
           + f"Arguments:\n{err.args}"
    return render_template('error.html', urlroot=request.url_root,
                           title=render_warning(errtype), message=mess)


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

# ******************************************************************************
# * Navigation utility functions                                               *
# ******************************************************************************

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

# ******************************************************************************
# * Payload utility functions                                                  *
# ******************************************************************************

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

# ******************************************************************************
# * ORCID utility functions                                                    *
# ******************************************************************************

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


def get_orcid_from_db(oid, use_eid=False):
    ''' Generate HTML for an ORCID or employeeId that is in the orcid collection
        Keyword arguments:
          oid: ORCID or employeeId
        Returns:
          HTML and a list of DOIs
    '''
    print("Lookup ORCID")
    try:
        orc = DL.single_orcid_lookup(oid, DB['dis'].orcid, 'employeeId' if use_eid else 'orcid')
    except Exception as err:
        raise CustomException(err, "Could not find_one in orcid collection by ORCID ID.") from err
    if not orc:
        return "", []
    badges = add_orcid_badges(orc)
    html = " ".join(badges)
    html += "<br><table class='borderless'>"
    html += f"<tr><td>Given name:</td><td>{', '.join(sorted(orc['given']))}</td></tr>"
    html += f"<tr><td>Family name:</td><td>{', '.join(sorted(orc['family']))}</td></tr>"
    if 'employeeId' in orc:
        link = "<a href='" + f"{app.config['WORKDAY']}{orc['userIdO365']}" \
               + f"' target='_blank'>{orc['employeeId']}</a>"
        html += f"<tr><td>Employee ID:</td><td>{link}</td></tr>"
    if 'affiliations' in orc:
        html += f"<tr><td>Affiliations:</td><td>{', '.join(orc['affiliations'])}</td></tr>"
    html += "</table><br>"
    payload = {"$and": [{"$or": [{"author.given": {"$in": orc['given']}},
                                 {"creators.givenName": {"$in": orc['given']}}]},
                        {"$or": [{"author.family": {"$in": orc['family']}},
                                 {"creators.familyName": {"$in": orc['family']}}]}]
              }
    try:
        rows = DB['dis'].dois.find(payload)
    except Exception as err:
        raise CustomException(err, "Could not find in dois collection by name.") from err
    works = []
    dois = []
    for row in rows:
        if row['doi']:
            doi = f"<a href='/doiui/{row['doi']}'>{row['doi']}</a>"
        else:
            doi = "&nbsp;"
        title = DL.get_title(row)
        dois.append(row['doi'])
        payload = {"date":  DL.get_publishing_date(row),
                   "doi": doi,
                   "title": title
                  }
        works.append(payload)
    if not works:
        return html, []
    html += '<table id="papers" class="tablesorter standard"><thead><tr>' \
            + '<th>Published</th><th>DOI</th><th>Title</th>' \
            + '</tr></thead><tbody>'

    for work in sorted(works, key=lambda row: row['date'], reverse=True):
        html += f"<tr><td>{work['date']}</td><td>{work['doi'] if work['doi'] else '&nbsp;'}</td>" \
                + f"<td>{work['title']}</td></tr>"
    if dois:
        html += "</tbody></table>"
    return html, dois


def add_orcid_works(data, dois):
    ''' Generate HTML for a list of works from ORCID
        Keyword arguments:
          data: ORCID data
          dois: list of DOIs from dois collection
        Returns:
          HTML for a list of works from ORCID
    '''
    html = inner = ""
    for work in data['activities-summary']['works']['group']:
        wsumm = work['work-summary'][0]
        date = get_work_publication_date(wsumm)
        doi = get_work_doi(work)
        if (not doi) or (doi in dois):
            continue
        if not doi:
            inner += f"<tr><td>{date}</td><td>&nbsp;</td>" \
                     + f"<td>{wsumm['title']['title']['value']}</td></tr>"
            continue
        if work['external-ids']['external-id'][0]['external-id-url']:
            if work['external-ids']['external-id'][0]['external-id-url']:
                link = "<a href='" \
                       + work['external-ids']['external-id'][0]['external-id-url']['value'] \
                       + f"' target='_blank'>{doi}</a>"
        else:
            link = f"<a href='/doiui/{doi}'>{doi}</a>"
        inner += f"<tr><td>{date}</td><td>{link}</td>" \
                 + f"<td>{wsumm['title']['title']['value']}</td></tr>"
    if inner:
        html += '<hr>The additional titles below are from ORCID. Note that titles below may ' \
                + 'be self-reported, and may not have DOIs available</br>'
        html += '<table id="works" class="tablesorter standard"><thead><tr>' \
                + '<th>Published</th><th>DOI</th><th>Title</th>' \
                + f"</tr></thead><tbody>{inner}</tbody></table>"
    return html


def generate_user_table(rows):
    ''' Generate a user table
    '''
    html = '<table id="ops" class="tablesorter standard"><thead><tr>' \
           + '<th>ORCID</th><th>Given name</th><th>Family name</th>' \
           + '</tr></thead><tbody>'
    for row in rows:
        if 'orcid' in row:
            link = f"<a href='/orcidui/{row['orcid']}'>{row['orcid']}</a>"
        elif 'employeeId' in row:
            link = f"<a href='/userui/{row['employeeId']}'>No ORCID found</a>"
        else:
            link = 'No ORCID found'
        html += f"<tr><td>{link}</td><td>{', '.join(row['given'])}</td>" \
                + f"<td>{', '.join(row['family'])}</td></tr>"
    html += '</tbody></table>'
    return html

# ******************************************************************************
# * DOI utility functions                                                      *
# ******************************************************************************

def get_doi(doi):
    ''' Add a table of custom JRC fields
        Keyword arguments:
          doi: DOI
        Returns:
          source: data source
          data: data from response
    '''
    if DL.is_datacite(doi):
        resp = JRC.call_datacite(doi)
        source = 'datacite'
        data = resp['data']['attributes'] if 'data' in resp else {}
    else:
        resp = JRC.call_crossref(doi)
        source = 'crossref'
        data = resp['message'] if 'message' in resp else {}
    return source, data


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


def add_relations(row):
    ''' Create a list of relations
        Keyword arguments:
          row: DOI record
        Returns:
          HTML
    '''
    html = ""
    if ("relation" not in row) or (not row['relation']):
        return html
    for rel in row['relation']:
        used = []
        for itm in row['relation'][rel]:
            if itm['id'] in used:
                continue
            link = f"<a href='/doiui/{itm['id']}'>{itm['id']}</a>"
            html += f"This DOI {rel.replace('-', ' ')} {link}<br>"
            used.append(itm['id'])
    return html


def get_migration_data(doi):
    ''' Create a migration record for a single DOI
        Keyword arguments:
          doi: DOI
        Returns:
          migration dictionary
    '''
    project = {'_id': 0}
    rec = {}
    try:
        row = DB['dis'].dois.find_one({"doi": doi}, project)
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    if not row:
        return rec
    # Author
    try:
        authors = DL.get_author_details(row, DB['dis'].orcid)
    except Exception as err:
        raise InvalidUsage("COuld not get author details: " + str(err), 500) from err
    tagname = []
    tags = []
    try:
        orgs = DL.get_supervisory_orgs()
    except Exception as err:
        raise InvalidUsage("Could not get suporgs: " + str(err), 500) from err
    if 'jrc_tag' in row:
        for atag in row['jrc_tag']:
            if atag not in tagname:
                code = orgs[atag] if atag in orgs else None
                tagname.append(atag)
                tags.append({"name": atag, "code": code})
        if tags:
            rec['tags'] = tags
    rec['authors'] = authors
    # Additional data
    if row['jrc_obtained_from'] == 'Crossref' and 'abstract' in row:
        rec['abstract'] = row['abstract']
    rec['journal'] = DL.get_journal(row)
    if 'jrc_publishing_date' in row:
        rec['jrc_publishing_date'] = row['jrc_publishing_date']
    if 'publisher' in row:
        rec['publisher'] = row['publisher']
    rec['title'] = DL.get_title(row)
    if 'URL' in row:
        rec['url'] = row['URL']
    return rec

# ******************************************************************************
# * Badge utility functions                                                    *
# ******************************************************************************

def tiny_badge(btype, msg, link=None):
    ''' Create HTML for a [very] small badge
        Keyword arguments:
          btype: badge type (success, danger, etc.)
          msg: message to show on badge
          link: link to other web page
        Returns:
          HTML
    '''
    html = f"<span class='badge badge-{btype}' style='font-size: 8pt'>{msg}</span>"
    if link:
        html = f"<a href='{link}' target='_blank'>{html}</a>"
    return html


def get_badges(auth):
    ''' Create a list of badges for an author
        Keyword arguments:
          auth: detailed author record
        Returns:
          List of HTML badges
    '''
    badges = []
    if auth['in_database']:
        badges.append(f"{tiny_badge('success', 'In database')}")
        if auth['alumni']:
            badges.append(f"{tiny_badge('danger', 'Alumni')}")
        elif 'validated' not in auth or not auth['validated']:
            badges.append(f"{tiny_badge('warning', 'Not validated')}")
        if 'orcid' not in auth or not auth['orcid']:
            badges.append(f"{tiny_badge('urgent', 'No ORCID')}")
        if auth['asserted']:
            badges.append(f"{tiny_badge('info', 'Janelia affiliation')}")
    else:
        badges.append(f"{tiny_badge('danger', 'Not in database')}")
        if auth['asserted']:
            badges.append(f"{tiny_badge('info', 'Janelia affiliation')}")
    return badges


def show_tagged_authors(authors):
    ''' Create a list of Janelian authors (with badges and tags)
        Keyword arguments:
          authors: list of detailed authors from a publication
        Returns:
          List of HTML authors
    '''
    alist = []
    for auth in authors:
        if (not auth['janelian']) and (not auth['asserted']):
            continue
        who = f"{auth['given']} {auth['family']}"
        if 'orcid' in auth and auth['orcid']:
            who = f"<a href='/orcidui/{auth['orcid']}'>{who}</a>"
        elif 'employeeId' in auth and auth['employeeId']:
            who = f"<a href='/userui/{auth['employeeId']}'>{who}</a>"
        badges = get_badges(auth)
        tags = []
        if 'group' in auth:
            tags.append(auth['group'])
        if 'tags' in auth:
            for tag in auth['tags']:
                if tag not in tags:
                    tags.append(tag)
        tags.sort()
        row = f"<td>{who}</td><td>{' '.join(badges)}</td><td>{', '.join(tags)}</td>"
        alist.append(row)
    return f"<table class='borderless'><tr>{'</tr><tr>'.join(alist)}</tr></table>"


def add_orcid_badges(orc):
    ''' Generate badges for an ORCID ID that is in the orcid collection
        Keyword arguments:
          orc: row from orcid collection
        Returns:
          List of badges
    '''
    badges = []
    badges.append(tiny_badge('success', 'In database'))
    if 'orcid' not in orc or not orc['orcid']:
        badges.append(f"{tiny_badge('urgent', 'No ORCID')}")
    if 'alumni' in orc:
        badges.append(tiny_badge('danger', 'Alumni'))
    if 'employeeId' not in orc:
        badges.append(tiny_badge('warning', 'Not validated'))
    return badges

# ******************************************************************************
# * General utility functions                                                  *
# ******************************************************************************

def random_string(strlen=8):
    ''' Generate a random string of letters and digits
        Keyword arguments:
          strlen: length of generated string
    '''
    components = string.ascii_letters + string.digits
    return ''.join(random.choice(components) for i in range(strlen))


def create_downloadable(name, header, content):
    ''' Generate a downloadable content file
        Keyword arguments:
          name: base file name
          header: table header
          content: table content
        Returns:
          File name
    '''
    fname = f"{name}_{random_string()}_{datetime.today().strftime('%Y%m%d%H%M%S')}.tsv"
    with open(f"/tmp/{fname}", "w", encoding="utf8") as text_file:
        text_file.write("\t".join(header) + "\n" + content)
    return f'<a class="btn btn-outline-success" href="/download/{fname}" ' \
                + 'role="button">Download tab-delimited file</a>'


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


def dloop(row, keys, sep="\t"):
    ''' Generate a string of joined velues from a dictionary
        Keyword arguments:
          row: dictionary
          keys: list of keys
          sep: separator
        Returns:
          Joined values from a dictionary
    '''
    return sep.join([str(row[fld]) for fld in keys])


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
# * Admin endpoints                                                           *
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
# * API endpoints (DOI)                                                        *
# ******************************************************************************
@app.route('/doi/authors/<path:doi>')
def show_doi_authors(doi):
    '''
    Return a DOI's authors
    Return information on authors for a given DOI.
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
    doi = doi.lstrip('/').rstrip('/').lower()
    result = initialize_result()
    try:
        coll = DB['dis'].dois
        row = coll.find_one({"doi": doi}, {'_id': 0})
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    if not row:
        result['data'] = []
        return generate_response(result)
    try:
        authors = DL.get_author_details(row, DB['dis'].orcid)
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    tagname = []
    tags = []
    try:
        orgs = DL.get_supervisory_orgs()
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    if 'jrc_tag' in row:
        for atag in row['jrc_tag']:
            if atag not in tagname:
                code = orgs[atag] if atag in orgs else None
                tagname.append(atag)
                tags.append({"name": atag, "code": code})
        if tags:
            result['tags'] = tags
    result['data'] = authors
    return generate_response(result)


@app.route('/doi/janelians/<path:doi>')
def show_doi_janelians(doi):
    '''
    Return a DOI's Janelia authors
    Return information on Janelia authors for a given DOI.
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
    result = initialize_result()
    resp = show_doi_authors(doi)
    data = resp.json
    result['data'] = []
    tags = []
    for auth in data['data']:
        if auth['janelian']:
            result['data'].append(auth)
            if 'tags' in auth:
                for atag in auth['tags']:
                    if atag not in tags:
                        tags.append(atag)
    if tags:
        tags.sort()
        result['tags'] = tags
    return generate_response(result)


@app.route('/doi/migration/<path:doi>')
def show_doi_migration(doi):
    '''
    Return a DOI's migration record
    Return migration information for a given DOI.
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
    doi = doi.lstrip('/').rstrip('/').lower()
    result = initialize_result()
    try:
        rec = get_migration_data(doi)
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    rec['doi'] = doi
    result['data'] = rec
    result['rest']['source'] = 'mongo'
    result['rest']['row_count'] = len(result['data'])
    return generate_response(result)


@app.route('/doi/migrations/<string:idate>')
def show_doi_migrations(idate):
    '''
    Return migration records for DOIs inserted since a specified date
    Return migration records for DOIs inserted since a specified date.
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
      500:
        description: MongoDB error
    '''
    result = initialize_result()
    try:
        isodate = datetime.strptime(idate,'%Y-%m-%d')
    except Exception as err:
        raise InvalidUsage(str(err), 400) from err
    try:
        rows = DB['dis'].dois.find({"jrc_inserted": {"$gte" : isodate}}, {'_id': 0})
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    result['rest']['row_count'] = 0
    result['rest']['source'] = 'mongo'
    result['data'] = []
    for row in rows:
        try:
            doi = row['doi']
            rec = get_migration_data(doi)
            rec['doi'] = doi
            result['data'].append(rec)
        except Exception as err:
            raise InvalidUsage(str(err), 500) from err
    result['rest']['row_count'] = len(result['data'])
    return generate_response(result)


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
    doi = doi.lstrip('/').rstrip('/').lower()
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
    result['rest']['source'], result['data'] = get_doi(doi)
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
        isodate = datetime.strptime(idate,'%Y-%m-%d')
    except Exception as err:
        raise InvalidUsage(str(err), 400) from err
    print(isodate)
    try:
        rows = DB['dis'].dois.find({"jrc_inserted": {"$gte" : isodate}}, {'_id': 0})
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
    doi = doi.lstrip('/').rstrip('/').lower()
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
            row = coll.find_one({"doi": doi.tolower()}, {'_id': 0})
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
    doi = doi.lstrip('/').rstrip('/').lower()
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
    doi = doi.lstrip('/').rstrip('/').lower()
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


@app.route('/doi/custom', methods=['OPTIONS', 'POST'])
def show_dois_custom():
    '''
    Return DOIs for a given find query
    Return a list of DOI records for a given query.
    ---
    tags:
      - DOI
    parameters:
      - in: query
        name: query
        schema:
          type: string
        required: true
        description: MongoDB query
    responses:
      200:
        description: DOI data
      500:
        description: MongoDB or formatting error
    '''
    result = initialize_result()
    ipd = receive_payload()
    if "query" not in ipd or not ipd['query']:
        raise InvalidUsage("You must specify a custom query")
    result['rest']['source'] = 'mongo'
    result['rest']['query'] = ipd['query']
    result['data'] = []
    coll = DB['dis'].dois
    try:
        rows = coll.find(ipd['query'], {'_id': 0})
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    if not rows:
        generate_response(result)
    for row in rows:
        result['data'].append(row)
        result['rest']['row_count'] += 1
    return generate_response(result)


@app.route('/components', defaults={'ctype': 'dis'}, methods=['OPTIONS', 'POST'])
@app.route('/components/<string:ctype>', methods=['OPTIONS', 'POST'])
def show_multiple_components(ctype='dis'):
    '''
    Return DOI components for a given tag
    Return a list of citation components for a given tag.
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
        name: tag
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
    if "tag" not in ipd or not (ipd['tag']) or not isinstance(ipd['tag'], str):
        raise InvalidUsage("You must specify a tag")
    result['rest']['source'] = 'mongo'
    result['data'] = []
    coll = DB['dis'].dois
    try:
        rows = coll.find({"jrc_tag": ipd['tag']}, {'_id': 0})
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


@app.route('/types')
def show_types():
    '''
    Show data types
    Return DOI data types, subtypes, and counts
    ---
    tags:
      - DOI
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


@app.route('/doi/jrc_author/<path:doi>', methods=['OPTIONS', 'POST'])
def set_jrc_author(doi):
    '''
    Update Janelia authors for a given DOI
    Update Janelia authors (as employee IDs) in "jrc_author" for a given DOI.
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
        description: Success
      500:
        description: MongoDB or formatting error
    '''
    doi = doi.lstrip('/').rstrip('/').lower()
    result = initialize_result()
    result['data'] = []
    try:
        row = DB['dis'].dois.find_one({"doi": doi}, {'_id': 0})
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    if not row:
        raise InvalidUsage(f"Could not find DOI {doi}", 400)
    result['rest']['row_count'] = 1
    try:
        authors = DL.get_author_details(row, DB['dis'].orcid)
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    jrc_author = []
    for auth in authors:
        if auth['janelian'] and 'employeeId' in auth and auth['employeeId']:
            jrc_author.append(auth['employeeId'])
    if not jrc_author:
        return generate_response(result)
    payload = {"$set": {"jrc_author": jrc_author}}
    try:
        res = DB['dis'].dois.update_one({"doi": doi}, payload)
    except Exception as err:
        raise InvalidUsage(str(err), 500) from err
    if hasattr(res, 'matched_count') and res.matched_count:
        if hasattr(res, 'modified_count') and res.modified_count:
            result['rest']['rows_updated'] = res.modified_count
        result['data'] = jrc_author
    return generate_response(result)

# ******************************************************************************
# * API endpoints (ORCID)                                                      *
# ******************************************************************************

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


# ******************************************************************************
# * UI endpoints (general)                                                     *
# ******************************************************************************
@app.route('/download/<string:fname>')
def download(fname):
    ''' Downloadable content
    '''
    try:
        return send_file('/tmp/' + fname, download_name=fname)  # pylint: disable=E1123
    except Exception as err:
        return render_template("error.html", urlroot=request.url_root,
                               title='Download error', message=err)


@app.route('/')
@app.route('/home')
def show_home():
    ''' Home
    '''
    response = make_response(render_template('home.html', urlroot=request.url_root,
                                             navbar=generate_navbar('Home')))
    return response

# ******************************************************************************
# * UI endpoints (DOI)                                                         *
# ******************************************************************************
@app.route('/doiui/<path:doi>')
def show_doi_ui(doi):
    ''' Show DOI
    '''
    doi = doi.lstrip('/').rstrip('/').lower()
    try:
        row = DB['dis'].dois.find_one({"doi": doi})
    except Exception as err:
        return inspect_error(err, 'Could not get DOI')
    if row:
        html = '<h5 style="color:lime">This DOI is saved locally in the Janelia database</h5>'
        html += add_jrc_fields(row)
    else:
        html = '<h5 style="color:red">This DOI is not saved locally in the ' \
               + 'Janelia database</h5><br>'
    _, data = get_doi(doi)
    if not data:
        return render_template('warning.html', urlroot=request.url_root,
                                title=render_warning("Could not find DOI", 'warning'),
                                message=f"Could not find DOI {doi}")
    authors = DL.get_author_list(data, orcid=True)
    if not authors:
        return render_template('error.html', urlroot=request.url_root,
                                title=render_warning("Could not generate author list"),
                                message=f"Could not generate author list for {doi}")
    title = DL.get_title(data)
    if not title:
        return render_template('error.html', urlroot=request.url_root,
                                title=render_warning("Could not find title"),
                                message=f"Could not find title for {doi}")
    citation = f"{authors} {title}."
    journal = DL.get_journal(data)
    if not journal:
        return render_template('error.html', urlroot=request.url_root,
                                title=render_warning("Could not find journal"),
                                message=f"Could not find journal for {doi}")
    link = f"<a href='https://dx.doi.org/{doi}' target='_blank'>{doi}</a>"
    rlink = f"/doi/{doi}"
    chead = 'Citation'
    if 'type' in data:
        chead += f" for {data['type'].replace('-', ' ')}"
        if 'subtype' in data:
            chead += f" {data['subtype'].replace('-', ' ')}"
    html += f"<h4>{chead}</h4><span class='citation'>{citation} {journal}." \
            + f"<br>DOI: {link}</span> {tiny_badge('primary', 'Raw data', rlink)}<br><br>"
    html += add_relations(data)
    if row:
        try:
            authors = DL.get_author_details(row, DB['dis'].orcid)
        except Exception as err:
            return inspect_error(err, 'Could not get author list details')
        alist = show_tagged_authors(authors)
        if alist:
            html += f"<br><h4>Janelia authors</h4><div class='scroll'>{''.join(alist)}</div>"
    response = make_response(render_template('general.html', urlroot=request.url_root,
                                             title=doi, html=html,
                                             navbar=generate_navbar('DOIs')))
    return response


@app.route('/dois_type')
def dois_type():
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
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get types from dois collection"),
                               message=error_message(err))
    html = '<table id="types" class="tablesorter standard"><thead><tr>' \
           + '<th>Source</th><th>Type</th><th>Subtype</th><th>Count</th>' \
           + '</tr></thead><tbody>'
    for row in rows:
        for field in ('source', 'type', 'subtype'):
            if field not in row['_id']:
                row['_id'][field] = ''
        html += f"<tr><td>{row['_id']['source']}</td><td>{row['_id']['type']}</td>" \
                + f"<td>{row['_id']['subtype']}</td><td>{row['count']:,}</td></tr>"
    html += '</tbody></table>'
    response = make_response(render_template('general.html', urlroot=request.url_root,
                                             title="DOI types", html=html,
                                             navbar=generate_navbar('DOIs')))
    return response


@app.route('/dois_publisher')
def dois_publisher():
    ''' Show publishers with counts
    '''
    payload = [{"$group": {"_id": {"publisher": "$publisher"},
                           "count": {"$sum": 1}}},
               {"$sort" : {"count": -1}}]
    try:
        coll = DB['dis'].dois
        rows = coll.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get publishers " \
                                                    + "from dois collection"),
                               message=error_message(err))
    html = '<table id="types" class="tablesorter standard"><thead><tr>' \
           + '<th>Publisher</th><th>Count</th>' \
           + '</tr></thead><tbody>'
    for row in rows:
        onclick = "onclick='nav_post(\"publisher\",\"" + row['_id']['publisher'] + "\")'"
        link = f"<a href='#' {onclick}>{row['_id']['publisher']}</a>"
        html += f"<tr><td>{link}</td><td>{row['count']:,}</td></tr>"
    html += '</tbody></table>'
    response = make_response(render_template('general.html', urlroot=request.url_root,
                                             title="DOI publishers", html=html,
                                             navbar=generate_navbar('DOIs')))
    return response


@app.route('/dois_tag')
def dois_tag():
    ''' Show tags with counts
    '''
    payload = [{"$unwind" : "$jrc_tag"},
               {"$project": {"_id": 0, "jrc_tag": 1}},
               {"$group": {"_id": {"tag": "$jrc_tag"}, "count":{"$sum": 1}}},
               {"$sort": {"_id.tag": 1}}
              ]
    try:
        coll = DB['dis'].dois
        rows = coll.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get tags from dois collection"),
                               message=error_message(err))
    html = '<table id="types" class="tablesorter standard"><thead><tr>' \
           + '<th>Tag</th><th>Count</th>' \
           + '</tr></thead><tbody>'
    for row in rows:
        onclick = "onclick='nav_post(\"jrc_tag\",\"" + row['_id']['tag'] + "\")'"
        link = f"<a href='#' {onclick}>{row['_id']['tag']}</a>"
        html += f"<tr><td>{link}</td><td>{row['count']:,}</td></tr>"
    html += '</tbody></table>'
    response = make_response(render_template('general.html', urlroot=request.url_root,
                                             title="DOI publishers", html=html,
                                             navbar=generate_navbar('DOIs')))
    return response


@app.route('/doiui/custom', methods=['OPTIONS', 'POST'])
def show_doiui_custom():
    '''
    Return DOIs for a given find query
    Return a list of DOI records for a given query.
    ---
    tags:
      - DOI
    parameters:
      - in: query
        name: field
        schema:
          type: string
        required: true
        description: MongoDB field
      - in: query
        name: value
        schema:
          type: string
        required: true
        description: field value
    responses:
      200:
        description: DOI data
      500:
        description: MongoDB or formatting error
    '''
    ipd = receive_payload()
    for key in ('field', 'value'):
        if key not in ipd or not ipd[key]:
            return render_template('error.html', urlroot=request.url_root,
                                   title=render_warning(f"Missing {key}"),
                                   message=f"You must specify a {key}")
    try:
        rows = DB['dis'].dois.find({ipd['field']: ipd['value']})
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get DOIs"),
                               message=error_message(err))
    if not rows:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("DOIs not found"),
                               message=f"No DOIs were found for {ipd['field']}={ipd['value']}")
    header = ['Published', 'DOI', 'Title']
    html = "<table id='dois' class='tablesorter standard'><thead><tr>" \
           + ''.join([f"<th>{itm}</th>" for itm in header]) + "</tr></thead><tbody>"
    works = []
    for row in rows:
        published = DL.get_publishing_date(row)
        title = DL.get_title(row)
        link = f"<a href='/doiui/{row['doi']}'>{row['doi']}</a>"
        works.append({"published": published, "link": link, "title": title, "doi": row['doi']})
    fileoutput = ""
    for row in sorted(works, key=lambda row: row['published'], reverse=True):
        html += "<tr><td>" + dloop(row, ['published', 'link', 'title'], "</td><td>") + "</td></tr>"
        row['title'] = row['title'].replace("\n", " ")
        fileoutput += dloop(row, ['published', 'doi', 'title']) + "\n"
    html += '</tbody></table>'
    html = create_downloadable(ipd['field'], header, fileoutput) + html
    response = make_response(render_template('general.html', urlroot=request.url_root,
                                             title=f"DOIs for {ipd['field']} {ipd['value']}",
                                             html=html, navbar=generate_navbar('DOIs')))
    return response


# ******************************************************************************
# * UI endpoints (ORCID)                                                       *
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
                               message=error_message(err))
    if 'person' not in data:
        return render_template('warning.html', urlroot=request.url_root,
                               title=render_warning(f"Could not find ORCID ID {oid}", 'warning'),
                               message=data['user-message'])
    name = data['person']['name']
    if name['credit-name']:
        who = f"{name['credit-name']['value']}"
    else:
        who = f"{name['given-names']['value']} {name['family-name']['value']}"
    try:
        orciddata, dois = get_orcid_from_db(oid)
    except CustomException as err:
        return render_template('error.html', urlroot=request.url_root,
                                title=render_warning(f"Could not find ORCID ID {oid}", 'error'),
                                message=error_message(err))
    if not orciddata:
        return render_template('warning.html', urlroot=request.url_root,
                               title=render_warning(f"Could not find ORCID ID {oid}", 'warning'),
                               message="Could not find any information for this ORCID ID")
    html = f"<h3>{who}</h3>{orciddata}"
    # Works
    if 'works' in data['activities-summary'] and data['activities-summary']['works']['group']:
        html += add_orcid_works(data, dois)
    response = make_response(render_template('general.html', urlroot=request.url_root,
                                             title=f"<a href='https://orcid.org/{oid}' " \
                                                   + f"target='_blank'>{oid}</a>", html=html,
                                             navbar=generate_navbar('ORCID')))
    return response


@app.route('/userui/<string:eid>')
def show_user_ui(eid):
    ''' Show user record by employeeId
    '''
    try:
        orciddata, _ = get_orcid_from_db(eid, use_eid=True)
    except CustomException as err:
        return render_template('error.html', urlroot=request.url_root,
                                title=render_warning(f"Could not find employee ID {eid}",
                                                     'warning'),
                                message=error_message(err))
    if not orciddata:
        return render_template('warning.html', urlroot=request.url_root,
                               title=render_warning(f"Could not find employee ID {eid}", 'warning'),
                               message="Could not find any information for this employee ID")
    response = make_response(render_template('general.html', urlroot=request.url_root,
                                             title=f"Employee ID {eid}", html=orciddata,
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
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not count names in dois collection"),
                               message=error_message(err))
    html = generate_user_table(rows)
    response = make_response(render_template('general.html', urlroot=request.url_root,
                                             title=f"Search term: {name}", html=html,
                                             navbar=generate_navbar('ORCID')))
    return response


@app.route('/orcid_tag')
def orcid_tag():
    ''' Show ORCID tags (affiliations) with counts
    '''
    payload = [{"$unwind" : "$affiliations"},
               {"$project": {"_id": 0, "affiliations": 1}},
               {"$group": {"_id": {"affiliation": "$affiliations"}, "count":{"$sum": 1}}},
               {"$sort": {"_id.affiliation": 1}}
              ]
    try:
        rows = DB['dis'].orcid.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get affiliations " \
                                                    + "from orcid collection"),
                               message=error_message(err))
    html = '<table id="types" class="tablesorter standard"><thead><tr>' \
           + '<th>Affiliation</th><th>Count</th>' \
           + '</tr></thead><tbody>'
    for row in rows:
        link = f"<a href='/affiliation/{row['_id']['affiliation']}'>{row['_id']['affiliation']}</a>"
        html += f"<tr><td>{link}</td><td>{row['count']:,}</td></tr>"
    html += '</tbody></table>'
    response = make_response(render_template('general.html', urlroot=request.url_root,
                                             title="ORCID affiliations", html=html,
                                             navbar=generate_navbar('ORCID')))
    return response


@app.route('/orcid_entry')
def orcid_entry():
    ''' Show ORCID users with counts
    '''
    payload = {"$and": [{"orcid": {"$exists": True}}, {"employeeId": {"$exists": True}}]}
    try:
        cntb = DB['dis'].orcid.count_documents(payload)
        payload = {"$and": [{"orcid": {"$exists": True}}, {"employeeId": {"$exists": False}}]}
        cnto = DB['dis'].orcid.count_documents(payload)
        payload = {"$and": [{"orcid": {"$exists": False}}, {"employeeId": {"$exists": True}}]}
        cnte = DB['dis'].orcid.count_documents(payload)
        cntj = DB['dis'].orcid.count_documents({"alumni": {"$exists": False}})
        cnta = DB['dis'].orcid.count_documents({"alumni": {"$exists": True}})
        cntf = DB['dis'].orcid.count_documents({"affiliations": {"$exists": False}})
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get affiliations " \
                                                    + "from orcid collection"),
                               message=error_message(err))
    total = cntj + cnta
    html = '<table id="types" class="tablesorter standard"><tbody>'
    html += f"<tr><td>Entries in collection</td><td>{total:,}</td></tr>"
    html += f"<tr><td>Entries in collection with ORCID and employee ID</td><td>{cntb:,}" \
            + f" ({cntb/total*100:.2f}%)</td></tr>"
    html += f"<tr><td>Entries in collection with ORCID only</td><td>{cnto:,}" \
            + f" ({cnto/total*100:.2f}%)</td></tr>"
    html += f"<tr><td>Entries in collection with employee ID only</td><td>{cnte:,}" \
            + f" ({cnte/total*100:.2f}%)</td></tr>"
    html += f"<tr><td>Entries in collection without affiliations</td><td>{cntf:,}" \
            + f" ({cntf/total*100:.2f}%)</td></tr>"
    html += f"<tr><td>Current Janelians</td><td>{cntj:,} ({cntj/total*100:.2f}%)</td></tr>"
    html += f"<tr><td>Alumni</td><td>{cnta:,} ({cnta/total*100:.2f}%)</td></tr>"
    html += '</tbody></table>'
    response = make_response(render_template('general.html', urlroot=request.url_root,
                                             title="ORCID entries", html=html,
                                             navbar=generate_navbar('ORCID')))
    return response


@app.route('/affiliation/<string:aff>')
def orcid_affiliation(aff):
    ''' Show ORCID tags (affiliations) with counts
    '''
    payload = {"jrc_tag": aff}
    try:
        cnt = DB['dis'].dois.count_documents(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not count affiliations " \
                                                    + "in dois collection"),
                               message=error_message(err))
    html = f"<p>Number of tagged DOIs: {cnt:,}</p>"
    payload = {"affiliations": aff}
    try:
        rows = DB['dis'].orcid.find(payload).collation({"locale": "en"}).sort("family", 1)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get affiliations from " \
                                                    + "orcid collection"),
                               message=error_message(err))
    html += generate_user_table(rows)
    response = make_response(render_template('general.html', urlroot=request.url_root,
                                             title=f"{aff} affiliation",
                                             html=html,
                                             navbar=generate_navbar('ORCID')))
    return response

# ******************************************************************************
# * UI endpoints (stats)                                                       *
# ******************************************************************************
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
            collection[cname] = {"docs": f"{stat['count']:,}",
                                 "size": humansize(stat['size']),
                                 "free": f"{free:.2f}",
                                 "idx": ", ".join(indices)
                                }
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not get collection stats"),
                               message=error_message(err))
    html = '<table id="collections" class="tablesorter standard"><thead><tr>' \
           + '<th>Collection</th><th>Documents</th><th>Size</th><th>Free space</th>' \
           + '<th>Indices</th></tr></thead><tbody>'
    for coll, val in sorted(collection.items()):
        html += f"<tr><td>{coll}</td><td>" + dloop(val, ['docs', 'size', 'free', 'idx'],
                                                   "</td><td>") + "</td></tr>"
    html += '</tbody></table>'
    response = make_response(render_template('general.html', urlroot=request.url_root,
                                             title="Database statistics", html=html,
                                             navbar=generate_navbar('Stats')))
    return response

# ******************************************************************************
# * Multi-role endpoints (ORCID)                                               *
# ******************************************************************************

@app.route('/groups')
def show_groups():
    '''
    Show group owners from ORCID
    Return records whose ORCIDs have a group
    ---
    tags:
      - ORCID
    responses:
      200:
        description: groups
      500:
        description: MongoDB error
    '''
    result = initialize_result()
    expected = 'html' if 'Accept' in request.headers \
                         and 'html' in request.headers['Accept'] else 'json'
    coll = DB['dis'].orcid
    payload = {"group": {"$exists": True}}
    try:
        rows = coll.find(payload, {'_id': 0}).sort("group", 1)
    except Exception as err:
        if expected == 'html':
            return render_template('error.html', urlroot=request.url_root,
                                   title=render_warning("Could not get groups from MongoDB"),
                                   message=error_message(err))
        raise InvalidUsage(str(err), 500) from err
    if expected == 'json':
        result['rest']['source'] = 'mongo'
        result['data'] = []
        for row in rows:
            result['data'].append(row)
        result['rest']['row_count'] = len(result['data'])
        return generate_response(result)
    html = '<table class="standard"><thead><tr><th>Name</th><th>ORCID</th><th>Group</th>' \
           + '<th>Affiliations</th></tr></thead><tbody>'
    for row in rows:
        print(row)
        if 'affiliations' not in row:
            row['affiliations'] = ''
        link = f"<a href='/orcidui/{row['orcid']}'>{row['orcid']}</a>" if 'orcid' in row else ''
        html += f"<tr><td>{row['given'][0]} {row['family'][0]}</td>" \
                + f"<td style='width: 180px'>{link}</td><td>{row['group']}</td>" \
                + f"<td>{', '.join(row['affiliations'])}</td></tr>"
    html += '</tbody></table>'
    return render_template('general.html', urlroot=request.url_root, title='Groups', html=html,
                           navbar=generate_navbar('ORCID'))

# *****************************************************************************

if __name__ == '__main__':
    if app.config["RUN_MODE"] == 'dev':
        app.run(debug=app.config["DEBUG"])
    else:
        app.run(debug=app.config["DEBUG"])
