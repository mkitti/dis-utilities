''' email_authors.py
    Email information on newly-added DOIs to authors
'''

import argparse
from datetime import datetime, timedelta
from operator import attrgetter
import sys
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Database
DB = {}
# DOI-level data
AUTHORLIST = {}
TAGLIST = {}
# Email
SENDER = 'datainfo@janelia.hhmi.org'
DEVELOPER = 'svirskasr@hhmi.org'
RECEIVERS = ['scarlettv@hhmi.org', 'svirskasr@hhmi.org']

def terminate_program(msg=None):
    ''' Terminate the program gracefully
        Keyword arguments:
          msg: error message or object
        Returns:
          None
    '''
    if msg:
        if not isinstance(msg, str):
            msg = f"An exception of type {type(msg).__name__} occurred. Arguments:\n{msg.args}"
        LOGGER.critical(msg)
    sys.exit(-1 if msg else 0)


def initialize_program():
    ''' Initialize program
        Keyword arguments:
          None
        Returns:
          None
    '''
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err:
        terminate_program(err)
    dbs = ['dis']
    for source in dbs:
        dbo = attrgetter(f"{source}.{ARG.MANIFOLD}.write")(dbconfig)
        LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, ARG.MANIFOLD, dbo.host, dbo.user)
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)


def get_citation(row):
    ''' Create a citation for a DOI
        Keyword arguments:
          row: row from the dois collection
        Returns:
          DIS-style citation
    '''

    authors = DL.get_author_list(row)
    title = DL.get_title(row)
    return f"{authors} {title}. https://doi.org/{row['doi']}."


def create_doilists(row):
    ''' Create an authorlist for a DOI
        Keyword arguments:
          row: row from the dois collection
        Returns:
          None
    '''
    if 'jrc_tag' in row:
        TAGLIST[row['doi']] = ", ".join(row['jrc_tag'])
    if 'jrc_author' not in row:
        return
    names = []
    for auth in row['jrc_author']:
        resp = JRC.call_people_by_id(auth)
        if not resp:
            continue
        names.append(' '.join([resp['nameFirstPreferred'], resp['nameLastPreferred']]))
    AUTHORLIST[row['doi']] = ", ".join(names)


def process_authors(authors, publications, cnt):
    ''' Create and send emails to each author with their resources
        Keyword arguments:
          authors: dictionary of authors and their citations
          publications: list of citations
          cnt: DOI count
        Returns:
          None
    '''
    # Individual author emails
    summary = ""
    for auth, val in authors.items():
        resp = JRC.call_people_by_id(auth)
        name = ' '.join([resp['nameFirstPreferred'], resp['nameLastPreferred']])
        email = DEVELOPER if ARG.TEST else resp['email']
        subject = "Your recent publication" if len(val['citations']) == 1 \
                  else "Your recent publications"
        text1 = "publication has been added" if len(val['citations']) == 1 \
                else "publications have been added"
        text = f"Hello {resp['nameFirstPreferred']},<br><br>" \
               + "This is an automated email from Janelia’s Data and Information Services " \
               + f"department (DIS). Your recent {text1} to our database. " \
               + "Please review that the metadata below are correct. " \
               + "<span style='font-weight: bold'>No action is required from you, but if you " \
               + "see an error, please let us know</span>.<br><br>"
        text += "<span style='font-weight: bold'>Tags:</span> There may be multiple redundant " \
                + "tags for the same lab,  project team, or support team. This is fine. Just " \
                + "let us know if there is a lab/team that is missing, or if we’ve included " \
                + "a lab/team that doesn’t belong.<br><br>"
        text += "<span style='font-weight: bold'>Janelia authors:</span> The employee names " \
                + "listed below may not correspond perfectly to the author names on the paper " \
                + "(e.g., Jane Doe / Janet P. Doe). This is fine. Just let us know if we’ve " \
                + "missed anyone, or if we’ve included someone we shouldn’t have."
        text += "<br><br>Thank you!<br><br>"
        for res in val['citations']:
            text += f"{res}"
            doi = val['dois'].pop(0)
            if doi in TAGLIST:
                text += f"<br><span style='font-weight: bold'>Tags:</span> {TAGLIST[doi]}"
            if doi in AUTHORLIST:
                text += "<br><span style='font-weight: bold'>Janelia authors:</span> " \
                        + f"{AUTHORLIST[doi]}"
            text += "<br><br>"
        summary += f"{name} has {len(val['citations'])} " \
                   + f"citation{'' if len(val['citations']) == 1 else 's'}<br>"
        if ARG.WRITE or ARG.TEST:
            JRC.send_email(text, SENDER, [email], subject, mime='html')
        LOGGER.info(f"Email sent to {name} ({email})")
    if not (ARG.WRITE or ARG.TEST):
        return
    # Summary email
    subject = "Emails have been sent to authors for recent publications"
    text = f"{subject}.<br>DOIs: {cnt}<br>Authors: {len(authors)}<br><br>"
    text += "<br><br>".join(publications)
    text += "<br><br>" + summary
    email = DEVELOPER if ARG.TEST else RECEIVERS
    JRC.send_email(text, SENDER, email, subject, mime='html')


def process_dois():
    ''' Find and process DOIs
        Keyword arguments:
          None
        Returns:
          None
    '''
    week_ago = (datetime.today() - timedelta(days=ARG.DAYS)).strftime("%Y-%m-%d")
    LOGGER.info(f"Finding DOIs from the last {ARG.DAYS} day{'' if ARG.DAYS == 1 else 's'} " \
                + f"({week_ago})")
    payload = {"jrc_newsletter": {"$gte": week_ago}, "jrc_author": {"$exists": True},
               "$or": [{"jrc_obtained_from": "Crossref"},
                       {"jrc_obtained_from": "DataCite",
                        "types.resourceTypeGeneral": {"$ne": "Dataset"}}]}
    try:
        cnt = DB['dis'].dois.count_documents(payload)
        rows = DB['dis'].dois.find(payload)
    except Exception as err:
        terminate_program(err)
    LOGGER.info(f"DOIs found: {cnt}")
    authors = {}
    publications = []
    for row in rows:
        citation = get_citation(row)
        publications.append(citation)
        for auth in row['jrc_author']:
            if auth not in authors:
                authors[auth] = {"citations": [], "dois": []}
            authors[auth]['citations'].append(citation)
            authors[auth]['dois'].append(row['doi'])
        if row['doi'] not in AUTHORLIST:
            create_doilists(row)
    LOGGER.info(f"Authors found: {len(authors)}")
    process_authors(authors, publications, cnt)

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Email information on newly-added DOIs to author")
    PARSER.add_argument('--days', dest='DAYS', action='store', type=int,
                        default=5, help='Number of days to go back for DOIs')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, prod)')
    PARSER.add_argument('--test', dest='TEST', action='store_true',
                        default=False, help='Send emails to developer')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Actually send emails')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    DISCONFIG = JRC.simplenamespace_to_dict(JRC.get_config("dis"))
    initialize_program()
    process_dois()
    terminate_program()
