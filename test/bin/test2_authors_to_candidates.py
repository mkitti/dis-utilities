# before running, put utility/bin in path like so:
# export PYTHONPATH="${PYTHONPATH}:/groups/scicompsoft/home/scarlettv/dis-utilities/utility/bin"

import db_connect
import jrc_common.jrc_common as JRC
import test_file_integrity as tfi
import name_match as nm


db_connect.initialize_program()
LOGGER = JRC.setup_logging(tfi.DummyArg()) 
orcid_collection = db_connect.DB['dis'].orcid
doi_collection = db_connect.DB['dis'].dois

config_file_obj = open('single_author/config.txt', 'r')
config_dict = {line.split(':')[0]: line.split(':')[1].rstrip('\n') for line in config_file_obj.readlines()}
config_file_obj.close()
config = tfi.TestCase(**config_dict)

doi_record = tfi.mimic_doi_common_get_doi_record(f'{config.dirname}/doi_record.txt')
author_list = tfi.mimic_doi_common_get_author_details(f'{config.dirname}/author_details.txt')
all_authors = [ nm.create_author(author_record) for author_record in author_list]

candidates = []
for a in all_authors:
    name = nm.HumanName(a.name)
    candidates = nm.name_search(name.first, name.last)
    
target = eval(config.initial_candidate_employee_ids)
if target == candidates:
    print('Pass: initial candidate employee IDs')
else:
    print(f'Fail: initial candidate employee IDs\nExpected:{config.initial_candidate_employee_ids}\nReturned:{candidates}')

