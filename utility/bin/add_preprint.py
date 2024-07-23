""" add_preprint.py
    Add jrc_preprint field to a DOIs. If the user doesn't specify a primary DOI and a preprint
    DOI, then the program will search for DOIs with the same title and two entries. It will then
    check if the two DOIs are a journal article and a preprint.
"""

__version__ = '1.0.0'

import argparse
import collections
from datetime import datetime
from operator import attrgetter
import sys
from tqdm import tqdm
import jrc_common.jrc_common as JRC

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Database
DB = {}
# Counters
COUNT = collections.defaultdict(lambda: 0, {})
# General
NAMES = {}
TITLE = []

def terminate_program(msg=None):
    ''' Terminate the program gracefully
        Keyword arguments:
          msg: error message
        Returns:
          None
    '''
    if msg:
        if not isinstance(msg, str):
            msg = f"An exception of type {type(msg).__name__} occurred. Arguments:\n{msg.args}"
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


def assign_names(row):
    ''' Get a first and last author name
        Keyword arguments:
          row: database row
        Returns:
          first and last author names
    '''
    # First author
    if 'given' in row['author'][0]:
        first_author = " ".join([row['author'][0]['given'], row['author'][0]['family']])
        NAMES[first_author] = {'given': row['author'][0]['given'],
                               'family': row['author'][0]['family']}
    else:
        first_author = row['author'][0]['family']
        NAMES[first_author] = {'family': row['author'][0]['family']}
    # Last author
    if 'given' in row['author'][-1]:
        last_author = " ".join([row['author'][-1]['given'], row['author'][-1]['family']])
        NAMES[last_author] = {'given': row['author'][-1]['given'],
                              'family': row['author'][-1]['family']}
    elif 'name' in row['author'][-1]:
        last_author = row['author'][-1]['name']
        NAMES[last_author] = {'family': row['author'][-1]['name']}
    else:
        last_author = row['author'][-1]['family']
        NAMES[last_author] = {'family': row['author'][-1]['name']}
    return first_author, last_author


def process_prelim_rows(prelim_rows):
    ''' Process preliminary rows to yield a final list of DOIs
        Keyword arguments:
          prelim_rows: preliminary rows dict
        Returns:
          List of final rows
    '''
    final_rows = []
    for row in prelim_rows.values():
        if len(row) != 2:
            continue
        if row[0]['type'] == 'journal-article':
            final_rows.append([row[0]['doi'], row[1]['doi']])
        else:
            final_rows.append([row[1]['doi'], row[0]['doi']])
        COUNT['pairs'] += 1
    return final_rows


def get_rows_by_title(title):
    ''' Get rows by title
        Keyword arguments:
          title: title to search for
        Returns:
          List of rows
    '''
    try:
        payload = {"title": title,
                   "type": {"$in": ["journal-article", "posted-content"]}}
        cnt = DB['dis'].dois.count_documents(payload)
        if cnt != 2:
            if cnt > 2:
                TITLE.append(f"{title} has {cnt} entries")
            return None
        rows = DB['dis'].dois.find(payload)
    except Exception as err:
        terminate_program(err)
    return rows


def search_single_name(name):
    ''' Search for a single name
        Keyword arguments:
          name: name to search for
        Returns:
          True if name found, False otherwise
    '''
    if name not in NAMES:
        terminate_program(f"Name {name} not found in NAMES dictionary")
    if 'given' in NAMES[name]:
        payload = {"given": NAMES[name]['given'], "family": NAMES[name]['family']}
    else:
        payload = {"family": NAMES[name]['family']}
    try:
        orc = DB['dis'].orcid.find_one(payload)
    except Exception as err:
        terminate_program(err)
    if not orc:
        return None
    return orc['orcid']


def compare_names(stored, author):
    ''' Compare names
        Keyword arguments:
          stored: stored name
          author: author name
        Returns:
          True if names match, False otherwise
    '''
    # A period after a middle initial is a pretty common difference...
    if ". " in stored:
        stored = stored.replace(". ", " ")
    if ". " in author:
        author = author.replace(". ", " ")
    if stored == author:
        return True
    # See if ORCIDs match for both names
    LOGGER.warning(f"Comparing names: {stored} {author}")
    stored_orc = search_single_name(stored)
    LOGGER.warning(f"  {stored} ORCID: {stored_orc}")
    author_orc = search_single_name(author)
    LOGGER.warning(f"  {author} ORCID: {author_orc}")
    return bool(stored_orc == author_orc)


def advanced_name_match(first_stored, last_stored, first_author, last_author):
    ''' Advanced name matching
        Keyword arguments:
          first_stored: stored first author name
          last_stored: stored last author name
          first_author: author first name
          last_author: author last name
        Returns:
          True if names match, False otherwise
    '''
    if not compare_names(first_stored, first_author):
        return False
    if not compare_names(last_stored, last_author):
        return False
    return True


def get_doi_pairs():
    ''' Get a list of DOI pairs
        Keyword arguments:
          None
        Returns:
          List of DOI pairs
    '''
    if ARG.DOI and ARG.PREPRINT:
        COUNT['pairs'] = 1
        return [(ARG.DOI, ARG.PREPRINT)]
    payload = [{"$group" : {"_id": "$title", "count": {"$sum": 1}}},
               {"$match": {"_id": {"$ne": None} , "count": {"$gt": 1}}},
               {"$sort": {"count": -1}},
               {"$project": {"title": "$_id", "_id": 0, "count": 1}}
              ]
    try:
        prelim_rows = DB['dis'].dois.aggregate(payload)
    except Exception as err:
        terminate_program(err)
    rows = []
    for row in prelim_rows:
        rows.append(row)
    prelim_rows = {}
    for title in tqdm(rows, desc="Processing duplicate titles"):
        rows = get_rows_by_title(title['title'][0])
        if not rows:
            continue
        have = first_stored = last_stored = first_doi = ""
        for row in rows:
            if 'jrc_preprint' in row or row['type'] not in ('journal-article', 'posted-content') \
               or row['type'] == have:
                break
            have = row['type']
            first_author, last_author = assign_names(row)
            if not first_stored:
                first_doi = row['doi']
                first_stored = first_author
                last_stored = last_author
            else:
                if first_stored != first_author or last_stored != last_author:
                    LOGGER.warning(f"Author name mismatch for {first_doi} {row['doi']}: " \
                                   + f"({first_stored} {last_stored}) " \
                                   + f"({first_author} {last_author})")
                    if not advanced_name_match(first_stored, last_stored, first_author,
                                               last_author):
                        break
            LOGGER.debug(row['title'][0])
            LOGGER.debug(f"  {row['doi']}  {have}  {first_author} {last_author}")
            if row['title'][0] not in prelim_rows:
                prelim_rows[row['title'][0]] = []
            prelim_rows[row['title'][0]].append(row)
        if not prelim_rows:
            continue
    final_rows = process_prelim_rows(prelim_rows)
    return final_rows


def process_pair(primary, preprint):
    ''' Process a pair of DOIs
        Keyword arguments:
          primary: primary DOI
          preprint: preprint DOI
        Returns:
          None
    '''
    dois = {}
    for doi in (primary, preprint):
        doi_type = 'primary' if doi == primary else 'preprint'
        try:
            row = DB['dis'].dois.find_one({"doi": doi.lower()})
        except Exception as err:
            terminate_program(err)
        if not row:
            terminate_program(f"{doi_type.capitalize()} DOI {doi} not found")
        if doi_type == 'primary':
            if 'jrc_preprint' in row:
                terminate_program(f"Primary DOI {doi} already has a preprint DOI " \
                                  + f"{row['jrc_preprint']}")
            if row['type'] != 'journal-article':
                terminate_program(f"Primary DOI {doi} type is {row['type']}, not a journal article")
        else:
            if 'jrc_preprint' in row:
                terminate_program(f"Preprint DOI {doi} already has a primary DOI " \
                                  + f"{row['jrc_preprint']}")
            if row['type'] != 'posted-content' or row['subtype'] != 'preprint':
                terminate_program(f"Preprint DOI {doi} type is {row['type']}, not a preprint")
        dois[doi_type] = row
    for doi_type in ('primary', 'preprint'):
        other = 'preprint' if doi_type == 'primary' else 'primary'
        if ARG.WRITE:
            try:
                DB['dis'].dois.update_one(
                    {"doi": dois[doi_type]['doi']},
                    {"$set": {"jrc_preprint": dois[other]['doi']}},
                    upsert=False)
            except Exception as err:
                terminate_program(err)
        LOGGER.info(f"Added {other} DOI {dois[other]['doi']} to {doi_type} " \
                    + f"DOI {dois[doi_type]['doi']}")
        COUNT['updated'] += 1


def add_jrc_preprint():
    """ Update jrc_preprint for specified DOIs
        Keyword arguments:
          None
        Returns:
          None
    """
    rows = get_doi_pairs()
    for pair in tqdm(rows, desc="Assigning preprints"):
        process_pair(pair[0], pair[1])
    if TITLE:
        file_name = 'titles_' + datetime.now().strftime('%Y-%m-%dT%H-%M-%S.txt')
        with open(file_name, 'w', encoding='utf-8') as ostream:
            for line in TITLE:
                ostream.write(f"{line}\n")
        LOGGER.warning(f"Titles with more than two entries written to {file_name}")
    print(f"DOI pairs found: {COUNT['pairs']:,}")
    print(f"DOIs updated: {COUNT['updated']:,}")
    if not ARG.WRITE:
        LOGGER.warning("Dry run successful, no updates were made")


# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Add jrc_preprint")
    PARSER.add_argument('--doi', dest='DOI', action='store',
                        help='Primary (non-preprint) DOI')
    PARSER.add_argument('--preprint', dest='PREPRINT', action='store',
                        help='Preprint DOI')
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
    add_jrc_preprint()
    terminate_program()
