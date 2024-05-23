''' update_orcid.py
    Update the MongoDB orcid collection with ORCiD IDs and names for Janelia authors
'''

import argparse
from operator import attrgetter
import re
import sys
import requests
from tqdm import tqdm
import jrc_common.jrc_common as JRC

# pylint: disable=broad-exception-caught

DB = {}
COUNT = {'records': 0, 'orcid': 0, 'insert': 0, 'update': 0}

def terminate_program(msg=None):
    ''' Terminate the program gracefully
        Keyword arguments:
          msg: error message or object
        Returns:
          None
    '''
    print(type(msg))
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
        except Exception as err: # pylint: disable=broad-exception-caught
            terminate_program(err)


def add_name(oid, oids, family, given):
    ''' If the ORCiD ID is new, add it to the dict. Otherwise, update it
        with new family/given name.
        Keyword arguments:
          oid: ORCiD ID
          oids: ORCiD ID dict
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


def process_author(aut, oids):
    ''' Process a single author record
        Keyword arguments:
          aut: author record
          oids: ORCiD ID dict
        Returns:
          None
    '''
    for aff in aut['affiliation']:
        if 'Janelia' in aff['name']:
            oid = re.sub(r'.*/', '', aut['ORCID'])
            add_name(oid, oids, aut['family'], aut['given'])
            break


def get_name(oid):
    ''' Get an author's first and last name from ORCiD
        Keyword arguments:
          oid: ORCiD
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
    ''' Find additional ORCiD IDs using the ORCiD API
        Keyword arguments:
          oids: ORCiD ID dict
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
    for oid in tqdm(authors, desc='Janelians from ORCiD'):
        family, given = get_name(oid)
        if family and given:
            add_name(oid, oids, family, given)
            continue


def write_records(oids):
    ''' Write records to Mongo
        Keyword arguments:
          oids: ORCiD ID dict
        Returns:
          None
    '''
    ocoll = DB['dis'].orcid
    for oid, val  in oids.items():
        if ARG.WRITE:
            result = ocoll.update_one({"orcid": oid}, {"$set": val}, upsert=True)
            if hasattr(result, 'matched_count') and result.matched_count:
                COUNT['update'] += 1
            else:
                COUNT['insert'] += 1
        else:
            print(oid, val)


def update_orcid():
    ''' Update the orcid collection
        Keyword arguments:
          None
        Returns:
          None
    '''
    # Get ORCiD IDs from the doi collection
    dcoll = DB['dis'].dois
    payload = {"author.affiliation.name": {"$regex": "Janelia"},
               "author.ORCID": {"$exists": True}}
    project = {"author.given": 1, "author.family": 1,
               "author.ORCID": 1, "author.affiliation": 1, "doi": 1}
    recs = dcoll.find(payload, project)
    oids = {}
    for rec in tqdm(recs, desc="Adding from doi collection"):
        COUNT['records'] += 1
        for aut in rec['author']:
            if 'ORCID' not in aut:
                continue
            process_author(aut, oids)
    add_from_orcid(oids)
    write_records(oids)
    print(f"Records read from MongoDB:dois: {COUNT['records']}")
    print(f"Records read from ORCiD:        {COUNT['orcid']}")
    print(f"ORCiD IDs inserted:             {COUNT['insert']}")
    print(f"ORCiD IDs updated:              {COUNT['update']}")

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Add ORCid information to MongoDB:orcid")
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='dev', choices=['dev', 'prod'],
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
    update_orcid()
    terminate_program()
