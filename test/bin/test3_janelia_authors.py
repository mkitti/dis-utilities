# before running, put utility/bin in path like so:
# export PYTHONPATH="${PYTHONPATH}:/groups/scicompsoft/home/scarlettv/dis-utilities/utility/bin"

# Run like so:
# python3 test3_janelia_authors.py <dir_name>
# python3 test3_janelia_authors.py single_author

import db_connect
import tc_common
import jrc_common.jrc_common as JRC
import doi_common.doi_common as doi_common
import sys

try:
    import name_match as nm
except:
    print('ERROR: Could not import name_match.py. Is it in your PYTHONPATH?')
    sys.exit(0)



# Boilerplate: initialize DB connection
db_connect.initialize_program()
LOGGER = JRC.setup_logging(db_connect.DummyArg()) 
orcid_collection = db_connect.DB['dis'].orcid
doi_collection = db_connect.DB['dis'].dois

#Boilerplate: create a TestCase object (attributes come from config file)
config = tc_common.TestCase()
config.read_config(sys.argv[1])


author_details_from_file = config.author_details()
author_details_from_dis = doi_common.get_author_details(config.doi_record(), doi_collection)  #IMPORTANT: NEED TO UPDATE THE SECOND ARG HERE... SOON

authors_from_file = [nm.create_author(a) for a in author_details_from_file]
authors_from_dis = [nm.create_author(a) for a in author_details_from_dis]

bool_results_from_dis = [nm.is_janelian(author, orcid_collection) for author in authors_from_dis]


target = dict(zip([a.name for a in authors_from_file], config.janelians_bool()))
test = dict(zip([a.name for a in authors_from_dis], bool_results_from_dis))


if target == test:
    print('Pass: assess whether Janelia is in the author affiliations')
else:
    print(f"Fail: assess whether Janelia is in the author affiliations\nResult from file: {target}\nResult from DIS DB: {test}")

