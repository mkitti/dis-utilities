''' update_orcid.py
    Update the MongoDB orcid collection with ORCID IDs and names for Janelia authors
'''

__version__ = '1.4.0'

import argparse
import collections
from datetime import datetime
import getpass
from operator import attrgetter
import os
import re
import sys
import requests
from tqdm import tqdm
import jrc_common.jrc_common as JRC

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

# Database
DB = {}
# Email
SENDER = 'svirskasr@hhmi.org'
RECEIVERS = ['scarlettv@hhmi.org', 'svirskasr@hhmi.org']
# Counters
#COUNT = {'records': 0, 'orcid': 0, 'insert': 0, 'update': 0}
COUNT = collections.defaultdict(lambda: 0, {})
# General
PRESENT = {}
NEW_ORCID = {}

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
    ''' Initialize database connection
        Keyword arguments:
          None
        Returns:
          None
    '''
    if "PEOPLE_API_KEY" not in os.environ:
        terminate_program("Missing token - set in PEOPLE_API_KEY environment variable")
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
        rows = DB['dis'].orcid.find({})
    except Exception as err:
        terminate_program(err)
    for row in rows:
        PRESENT[row['orcid']] = row
    LOGGER.info(f"{len(PRESENT)} DOIs are already in the collection")


def add_name(oid, oids, family, given):
    ''' If the ORCID ID is new, add it to the dict. Otherwise, update it
        with new family/given name.
        Keyword arguments:
          oid: ORCID ID
          oids: ORCID ID dict
          family: family name
          given: given name
        Returns:
          None
    '''
    if oid in oids:
        if family not in oids[oid]['family']:
            oids[oid]['family'].append(family)
        if given not in oids[oid]['given']:
            oids[oid]['given'].append(given)
    else:
        oids[oid] = {"family": [family], "given": [given]}
        if oid in PRESENT:
            if not ARG.WRITE:
                COUNT['update'] += 1
        else:
            if not ARG.WRITE:
                COUNT['insert'] += 1
            NEW_ORCID[oid] = {"family": [family], "given": [given]}


def process_author(aut, oids, source="crossref"):
    ''' Process a single author record
        Keyword arguments:
          aut: author record
          oids: ORCID ID dict
        Returns:
          None
    '''
    for aff in aut['affiliation']:
        if 'Janelia' in aff['name']:
            oid = re.sub(r'.*/', '', aut['ORCID'])
            if source == "crossref":
                add_name(oid, oids, aut['family'], aut['given'])
            break


def get_name(oid):
    ''' Get an author's first and last name from ORCID
        Keyword arguments:
          oid: ORCID
        Returns:
          family and given name
    '''
    url = f"https://pub.orcid.org/v3.0/{oid}"
    try:
        resp = requests.get(url, timeout=10,
                            headers={"Accept": "application/json"})
    except Exception as err:
        terminate_program(err)
    try:
        return resp.json()['person']['name']['family-name']['value'], \
               resp.json()['person']['name']['given-names']['value']
    except Exception as err:
        LOGGER.warning(resp.json()['person']['name'])
        LOGGER.warning(err)
        return None, None


def add_from_orcid(oids):
    ''' Find additional ORCID IDs using the ORCID API
        Keyword arguments:
          oids: ORCID ID dict
        Returns:
          None
    '''
    authors = []
    base = 'https://pub.orcid.org/v3.0/search'
    for url in ('/?q=ror-org-id:"https://ror.org/013sk6x84"',
                '/?q=affiliation-org-name:"Janelia Research Campus"',
                '/?q=affiliation-org-name:"Janelia Farm Research Campus"'):
        try:
            resp = requests.get(f"{base}{url}", timeout=10,
                                headers={"Accept": "application/json"})
        except Exception as err:
            terminate_program(err)
        for orcid in resp.json()['result']:
            authors.append(orcid['orcid-identifier']['path'])
    COUNT['orcid'] = len(authors)
    for oid in tqdm(authors, desc='Janelians from ORCID'):
        family, given = get_name(oid)
        if family and given:
            add_name(oid, oids, family, given)


def people_by_name(first, surname):
    ''' Search for a name in the people system
        Keyword arguments:
          first: first name
          surname: last name
        Returns:
          List of people
    '''
    try:
        people = JRC.call_people_by_name(surname)
    except Exception as err:
        terminate_program(err)
    filtered = []
    for person in people:
        if person['locationName'] != 'Janelia Research Campus':
        # or person['photoURL'].endswith('PlaceHolder.png'):
            continue
        if person['nameLastPreferred'].lower() == surname.lower() \
           and person['nameFirstPreferred'].lower() == first.lower():
            filtered.append(person)
    return filtered


def process_middle_initials(oids, oid):
    ''' Add name combinations for first names in the forms "F. M." or "F."
        with or without the periods.
        Keyword arguments:
          oid: ORCID ID
          oids: ORCID ID dict
        Returns:
          None
    '''
    for first in oids[oid]['given']:
        if re.search(r"[A-Za-z]\. [A-Za-z]\.$", first):
            continue
        if re.search(r"[A-Za-z]\.[A-Za-z]\.$", first):
            new = first.replace('.', ' ')
            if new not in oids[oid]['given']:
                oids[oid]['given'].append(new)
        elif re.search(r" [A-Za-z]\.$", first):
            new = first.rstrip('.')
            if new not in oids[oid]['given']:
                oids[oid]['given'].append(new)


def find_name_combos(idresp, oids, oid):
    ''' Add name combinations
        Keyword arguments:
          idresp: record from HHMI's People service
          oid: ORCID ID
          oids: ORCID ID dict
        Returns:
          None
    '''
    if idresp:
        for source in ('nameFirst', 'nameFirstPreferred'):
            if source in idresp and idresp[source] and idresp[source] not in oids[oid]['given']:
                oids[oid]['given'].append(idresp[source])
        for source in ('nameLast', 'nameLastPreferred'):
            if source in idresp and idresp[source] and idresp[source] not in oids[oid]['family']:
                oids[oid]['family'].append(idresp[source])
        for source in ('nameMiddle', 'nameMiddlePreferred'):
            if source not in idresp or not idresp[source]:
                continue
            for first in oids[oid]['given']:
                if ' ' in first:
                    continue
                new = f"{first} {idresp[source][0]}"
                if new not in oids[oid]['given']:
                    oids[oid]['given'].append(new)
                new += '.'
                if new not in oids[oid]['given']:
                    oids[oid]['given'].append(new)
    process_middle_initials(oids, oid)


def find_affiliations(first, surname, idresp, oids, oid):
    ''' Add affiliations
        Keyword arguments:
          first: given name
          surname: family name
          idresp: record from HHMI's People service
          oid: ORCID ID
          oids: ORCID ID dict
        Returns:
          None
    '''
    if idresp:
        if 'affiliations' in idresp and idresp['affiliations']:
            oids[oid]['affiliations'] = []
            for aff in idresp['affiliations']:
                if aff['supOrgName'] not in oids[oid]['affiliations']:
                    oids[oid]['affiliations'].append(aff['supOrgName'])
    if 'affiliations' in oids[oid]:
        LOGGER.info(f"Added {first} {surname} from People " \
                    + f"({', '.join(oids[oid]['affiliations'])})")
    else:
        LOGGER.info(f"Added {first} {surname} from People")


def add_people_information(first, surname, oids, oid):
    ''' Correlate a name from ORCID with HHMI's People service
        Keyword arguments:
          first: given name
          surname: family name
          oid: ORCID ID
          oids: ORCID ID dict
        Returns:
          None
    '''
    found = False
    people = people_by_name(first, surname)
    if people:
        if len(people) == 1:
            found = True
            oids[oid]['employeeId'] = people[0]['employeeId']
            oids[oid]['userIdO365'] = people[0]['userIdO365']
            if 'group leader' in people[0]['businessTitle'].lower():
                oids[oid]['group'] = f"{first} {surname} Lab"
            elif people[0]['businessTitle'] == 'JRC Alumni':
                oids[oid]['alumni'] = True
            idresp = JRC.call_people_by_id(oids[oid]['employeeId'])
            find_name_combos(idresp, oids, oid)
            find_affiliations(first, surname, idresp, oids, oid)
        else:
            LOGGER.error(f"Found more than one record in People for {first} {surname}")
    return found


def correlate_person(oid, oids):
    ''' Correlate a name from ORCID with HHMI's People service
        Keyword arguments:
          oid: ORCID ID
          oids: ORCID ID dict
        Returns:
          None
    '''
    val = oids[oid]
    for surname in val['family']:
        for first in val['given']:
            found = add_people_information(first, surname, oids, oid)
            if found:
                break
        if found:
            break
    #if not found:
    #    LOGGER.warning(f"Could not find a record in People for {first} {surname}")


def preserve_mongo_names(current, oids):
    ''' Preserve names from sources other than this program in the oids dictionary
        Keyword arguments:
          oids: ORCID ID dict
        Returns:
          None
    '''
    oid = current['orcid']
    for field in ('family', 'given'):
        for name in current[field]:
            if name not in oids[oid][field]:
                oids[oid][field].append(name)


def add_janelia_info(oids):
    ''' Find Janelia information for each ORCID ID
        Keyword arguments:
          oids: ORCID ID dict
        Returns:
          None
    '''
    for oid in tqdm(oids, desc='Janalians from orcid collection'):
        if oid in PRESENT:
            preserve_mongo_names(PRESENT[oid], oids)
        if oid in PRESENT and 'employeeId' in PRESENT[oid]:
            continue
        correlate_person(oid, oids)


def write_records(oids):
    ''' Write records to Mongo
        Keyword arguments:
          oids: ORCID ID dict
        Returns:
          None
    '''
    coll = DB['dis'].orcid
    for oid, val in tqdm(oids.items(), desc='Updating orcid collection'):
        result = coll.update_one({"orcid": oid}, {"$set": val}, upsert=True)
        if hasattr(result, 'matched_count') and result.matched_count:
            COUNT['update'] += 1
        else:
            COUNT['insert'] += 1
            print(f"New entry: {val}")


def generate_email():
    ''' Generate and send an email
        Keyword arguments:
          None
        Returns:
          None
    '''
    msg = ""
    user = getpass.getuser()
    if user:
        try:
            workday = JRC.simplenamespace_to_dict(JRC.get_config("workday"))
        except Exception as err:
            terminate_program(err)
        if user in workday:
            rec = workday[user]
            msg += f"Program (version {__version__}) run by {rec['first']} {rec['last']} " \
                   + f"at {datetime.now()}\n"
        else:
            msg += f"Program (version {__version__}) run by {user} at {datetime.now()}\n"
    msg += f"The following ORCID IDs were inserted into the {ARG.MANIFOLD} MongoDB DIS database:"
    for oid, val in NEW_ORCID.items():
        msg += f"\n{oid}: {val}"
    try:
        LOGGER.info(f"Sending email to {RECEIVERS}")
        JRC.send_email(msg, SENDER, ['svirskasr@hhmi.org'] if ARG.MANIFOLD == 'dev' else RECEIVERS,
                       "New ORCID IDs")
    except Exception as err:
        LOGGER.error(err)


def update_orcid():
    ''' Update the orcid collection
        Keyword arguments:
          None
        Returns:
          None
    '''
    LOGGER.info(f"Started run (version {__version__})")
    oids = {}
    if ARG.ORCID:
        family, given = get_name(ARG.ORCID)
        if family and given:
            add_name(ARG.ORCID, oids, family, given)
            COUNT['orcid'] += 1
            add_janelia_info(oids)
    else:
        # Get ORCID IDs from the doi collection
        dcoll = DB['dis'].dois
        # Crossref
        payload = {"author.affiliation.name": {"$regex": "Janelia"},
                   "author.ORCID": {"$exists": True}}
        project = {"author.given": 1, "author.family": 1,
                   "author.ORCID": 1, "author.affiliation": 1, "doi": 1}
        recs = dcoll.find(payload, project)
        for rec in tqdm(recs, desc="Adding from doi collection"):
            COUNT['records'] += 1
            for aut in rec['author']:
                if 'ORCID' not in aut:
                    continue
                process_author(aut, oids, "crossref")
        add_from_orcid(oids)
        add_janelia_info(oids)
    if ARG.WRITE:
        write_records(oids)
        if NEW_ORCID:
            generate_email()
    print(f"Records read from MongoDB:dois: {COUNT['records']}")
    print(f"Records read from ORCID:        {COUNT['orcid']}")
    print(f"ORCID IDs inserted:             {COUNT['insert']}")
    print(f"ORCID IDs updated:              {COUNT['update']}")

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Add ORCID information to MongoDB:orcid")
    PARSER.add_argument('--orcid', dest='ORCID', action='store',
                        help='ORCID ID')
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
    REST = JRC.get_config("rest_services")
    update_orcid()
    terminate_program()
