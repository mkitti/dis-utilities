import jrc_common.jrc_common as JRC
import doi_common.doi_common as DL
from operator import attrgetter
import sys

DB = {}
PROJECT = {}

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
        manifold = 'prod'
        dbo = attrgetter(f"{source}.{manifold}.write")(dbconfig)
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

def terminate_program(msg=None):
    ''' Terminate the program gracefully
        Keyword arguments:
          msg: error message
        Returns:
          None
    '''
    if msg:
        print(msg)
        sys.exit(-1)
    else:
        sys.exit(0)


# -----------------------------------------------------------------------------

if __name__ == '__main__':
    initialize_program()
    collection = DB['dis'].orcid
    print(DL.single_orcid_lookup('0000-0002-4156-2849', collection, 'orcid'))