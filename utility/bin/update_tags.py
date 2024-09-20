""" update_tags.py
    Update tags for selected DOIs
"""

__version__ = '1.0.0'

import argparse
import collections
from datetime import datetime, timedelta
from operator import attrgetter
import sys
import inquirer
from inquirer.themes import BlueComposure
import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Database
DB = {}
PROJECT = {}
# Counters
COUNT = collections.defaultdict(lambda: 0, {})

def terminate_program(msg=None):
    ''' Terminate the program gracefully
        Keyword arguments:
          msg: error message
        Returns:
          None
    '''
    if msg:
        LOGGER.critical(msg)
    sys.exit(-1 if msg else 0)


def initialize_program():
    ''' Intialize the program
        Keyword arguments:
          None
        Returns:
          None
    '''
    # Database
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
    try:
        rows = DB['dis'].project_map.find({})
    except Exception as err:
        terminate_program(err)
    for row in rows:
        PROJECT[row['name']] = row['project']


def get_dois():
    ''' Get a list of DOIs to process. This will be one of four things:
        - a single DOI from ARG.DOI
        - a list of DOIs from ARG.FILE
        Keyword arguments:
          None
        Returns:
          List of DOIs
    '''
    if ARG.DOI:
        COUNT['specified'] = 1
        return [ARG.DOI]
    if ARG.FILE:
        COUNT['specified'] = 1
        return ARG.FILE.read().splitlines()
    LOGGER.info(f"Finding DOIs from the last {ARG.DAYS} day{'' if ARG.DAYS == 1 else 's'}")
    week_ago = (datetime.today() - timedelta(days=ARG.DAYS))
    try:
        rows = DB['dis'].dois.find({"jrc_inserted": {"$gte": week_ago}})
    except Exception as err:
        terminate_program(err)
    dois = []
    for row in rows:
        dois.append(row['doi'])
    COUNT['specified'] = len(dois)
    return dois


def get_tags(authors):
    """ Get tags from a list of authors
        Keyword arguments:
          authors: list of detailed authors
        Returns:
          tags: list of tags
          janelians: list of Janelia author names
          tagauth: dict of authors by tag
    """
    tags = []
    janelians = []
    tagauth = {}
    for auth in authors:
        atags = []
        if auth['janelian']:
            janelians.append(f"{auth['given']} {auth['family']}")
        if 'group' in auth:
            if auth['group'] not in atags:
                atags.append(auth['group'])
        if 'tags' in auth:
            for tag in auth['tags']:
                if tag not in atags:
                    atags.append(tag)
        if 'name' in auth:
            if auth['name'] not in PROJECT:
                LOGGER.warning(f"Project {auth['name']} is not defined")
            elif PROJECT[auth['name']] and PROJECT[auth['name']] not in atags:
                atags.append(PROJECT[auth['name']])
        for tag in atags:
            if tag not in tags:
                tags.append(tag)
            if tag not in tagauth:
                tagauth[tag] = []
            if auth['family'] not in tagauth[tag]:
                tagauth[tag].append(auth['family'])
                tagauth[tag].sort()
    return tags, janelians, tagauth


def update_single_doi(rec):
    """ Update tags for a single DOI
        Keyword arguments:
          rec: DOI record
        Returns:
          None
    """
    authors = DL.get_author_details(rec, DB['dis'].orcid)
    current = []
    tags, janelians, tagauth = get_tags(authors)
    if not tags:
        LOGGER.warning(f"No tags for DOI {rec['doi']}")
        return
    tags.sort()
    tagd = {}
    for tag in tags:
        newtag = f"{tag} ({', '.join(tagauth[tag])})"
        if 'jrc_tag' in rec and tag in rec['jrc_tag']:
            current.append(newtag)
        tagd[newtag] = tag
    print(f"DOI: {rec['doi']}")
    print(f"{DL.get_title(rec)}")
    print(', '.join(janelians))
    today = datetime.today().strftime('%Y-%m-%d')
    quest = [inquirer.Checkbox('checklist', carousel=True,
                               message='Select tags',
                               choices=tagd, default=current),
             inquirer.List('newsletter',
                           message=f"Set jrc_newsletter to {today}",
                           choices=['Yes', 'No'])
            ]
    ans = inquirer.prompt(quest, theme=BlueComposure())
    tags = []
    for tag in ans['checklist']:
        tags.append(tagd[tag])
    payload = {"jrc_tag": tags}
    if 'newsletter' in ans and ans['newsletter'] == 'Yes':
        payload['jrc_newsletter'] = today
    COUNT['selected'] += 1
    if ARG.WRITE:
        coll = DB['dis'].dois
        result = coll.update_one({"doi": rec['doi']}, {"$set": payload})
        if hasattr(result, 'matched_count') and result.matched_count:
            COUNT['updated'] += 1
        if not tags:
            result = coll.update_one({"doi": rec['doi']}, {"$unset": {"jrc_tag":1}})
    else:
        print(f"{rec['doi']} {payload}")
        COUNT['updated'] += 1


def update_tags():
    """ Update tags for specified DOIs
        Keyword arguments:
          None
        Returns:
          None
    """
    LOGGER.info(f"Started run (version {__version__})")
    dois = get_dois()
    if not dois:
        terminate_program("No DOIs were found")
    coll = DB['dis'].dois
    for odoi in dois:
        doi = odoi.lower().strip()
        try:
            rec = coll.find_one({"doi": doi})
        except Exception as err:
            terminate_program(err)
        update_single_doi(rec)
    print(f"DOIs specified:           {COUNT['specified']}")
    print(f"DOIs selected for update: {COUNT['selected']}")
    print(f"DOIs updated:             {COUNT['updated']}")
    if not ARG.WRITE:
        LOGGER.warning("Dry run successful, no updates were made")

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Update tags")
    PARSER.add_argument('--doi', dest='DOI', action='store',
                        help='Single DOI to process')
    PARSER.add_argument('--file', dest='FILE', action='store',
                        type=argparse.FileType("r", encoding="ascii"),
                        help='File of DOIs to process')
    PARSER.add_argument('--days', dest='DAYS', action='store', type=int,
                        default=7, help='Number of days to go back for DOIs')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'],
                        help='MongoDB manifold (dev, prod)')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Write to database/config system')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    update_tags()
    terminate_program()
