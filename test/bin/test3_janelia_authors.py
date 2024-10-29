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



# Boilerplate: initiate DB connection
db_connect.initialize_program()
LOGGER = JRC.setup_logging(db_connect.DummyArg()) 
orcid_collection = db_connect.DB['dis'].orcid
doi_collection = db_connect.DB['dis'].dois

#Boilerplate: create a TestCase object (attributes come from config file)
config_dict = tc_common.read_config('single_author')
config = tc_common.TestCase(**config_dict)

doi_rec_from_file = tc_common.evaluate_file(f'{config.dirname}/doi_record.txt')
author_list_from_file = tc_common.evaluate_file(f'{config.dirname}/author_details.txt')
author_list_from_dis = doi_common.get_author_details(doi_rec_from_file, doi_collection)  #IMPORTANT: NEED TO UPDATE THE SECOND ARG HERE... SOON

authors_from_file = [nm.create_author(a) for a in author_list_from_file]
authors_from_dis = [nm.create_author(a) for a in author_list_from_file]

bool_results_from_file = []
for author in authors_from_file:
    bool_results_from_file.append(nm.is_janelian(author, orcid_collection))

bool_results_from_dis = []
for author in authors_from_dis:
    bool_results_from_dis.append(nm.is_janelian(author, orcid_collection))

if bool_results_from_file == bool_results_from_dis:
    print('Pass: assess whether Janelia is in the author affiliations')
else:
    print(f"Fail: assess whether Janelia is in the author affiliations\nResult from file:{dict(zip([a.name for a in authors_from_file], bool_results_from_file))}\nResult from DIS DB: {dict(zip([a.name for a in authors_from_file], bool_results_from_file))}")

