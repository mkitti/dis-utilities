''' update_orcid.py
    Update the MongoDB orcid collection with ORCID IDs and names for Janelia authors
'''

__version__ = '1.1.0'

import argparse
from operator import attrgetter
import os
import re
import sys
import requests
from tqdm import tqdm
import jrc_common.jrc_common as JRC

# pylint: disable=broad-exception-caught,logging-fstring-interpolation

DB = {}
COUNT = {'records': 0, 'orcid': 0, 'insert': 0, 'update': 0}

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
      terminate_program(f"Missing token - set in PEOPLE_API_KEY environment variable")
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
    headers = {'Content-Type': 'application/json', 'APIKey': os.environ['PEOPLE_API_KEY']}
    url = f"{attrgetter('people.url')(REST)}People/Search/ByName/{surname}"
    try:
        response = requests.get(url, headers=headers, timeout=10)
    except Exception as err:
        terminate_program(err)
    people = response.json()
    filtered = []
    for person in people:
        if person['locationName'] != 'Janelia Research Campus':
        # or person['photoURL'].endswith('PlaceHolder.png'):
            continue
        if person['nameLastPreferred'].lower() == surname.lower() \
           and person['nameFirstPreferred'].lower() == first.lower():
            filtered.append(person)
    return filtered


def people_by_id(eid):
    ''' Search for an employee ID in the people system
        Keyword arguments:
          eid: employee ID
        Returns:
          JSON from people system
    '''
    headers = {'Content-Type': 'application/json', 'APIKey': os.environ['PEOPLE_API_KEY']}
    url = f"{attrgetter('people.url')(REST)}People/Person/GetById/{eid}"
    try:
        response = requests.get(url, headers=headers, timeout=10)
        print(response)
        return response.json()
    except Exception as err:
        LOGGER.error(f"Could not get response from {url}")
        terminate_program(err)


def correlate_person(oid, oids):
    ''' Correlate a name from ORCID with HHMI's People service
        Keyword arguments:
          oid: ORCID ID
          oids: ORCID ID dict
        Returns:
          None
    '''
    val = oids[oid]
    found = False
    for surname in val['family']:
        for first in val['given']:
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
                    idresp = people_by_id(oids[oid]['employeeId'])
                    if idresp and 'affiliations' in idresp and idresp['affiliations']:
                        oids[oid]['affiliations'] = []
                        for aff in idresp['affiliations']:
                            if aff['supOrgName'] not in oids[oid]['affiliations']:
                                oids[oid]['affiliations'].append(aff['supOrgName'])
                    if 'affiliations' in oids[oid]:
                        LOGGER.info(f"Added {first} {surname} from People ({', '.join(oids[oid]['affiliations'])})")
                    else:
                        LOGGER.info(f"Added {first} {surname} from People")
                    break
                LOGGER.error(f"Found more than one record in People for {first} {surname}")
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
    present = {}
    try:
        coll = DB['dis'].orcid
        rows = coll.find({})
    except Exception as err:
        terminate_program(err)
    for row in rows:
        present[row['orcid']] = row
    for oid in tqdm(oids, desc='Janalia info'):
        if oid in present:
            preserve_mongo_names(present[oid], oids)
        if oid in present and 'employeeId' in present[oid]:
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
