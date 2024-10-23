# before running, put utility/bin in path like so:
# export PYTHONPATH="${PYTHONPATH}:/groups/scicompsoft/home/scarlettv/dis-utilities/utility/bin"

import db_connect
import tc_common
import jrc_common.jrc_common as JRC
import test_file_integrity as tfi
import name_match as nm



db_connect.initialize_program()
LOGGER = JRC.setup_logging(db_connect.DummyArg()) 
orcid_collection = db_connect.DB['dis'].orcid
doi_collection = db_connect.DB['dis'].dois

config_dict = tc_common.read_config('single_author')
config = tc_common.TestCase(**config_dict)


doi_record = tc_common.evaluate_file(f'{config.dirname}/doi_record.txt')
author_list = tc_common.evaluate_file(f'{config.dirname}/author_details.txt')
all_authors = [ nm.create_author(author_record) for author_record in author_list]

ids = []
for a in all_authors:
    name = nm.HumanName(a.name)
    ids = nm.name_search(name.first, name.last)
    
target = eval(config.initial_candidate_employee_ids)
if target == ids:
    print('Pass: initial candidate employee IDs')
else:
    print(f'Fail: initial candidate employee IDs\nExpected:{config.initial_candidate_employee_ids}\nReturned:{ids}')

nm_employees = []
for id in ids:
    nm.create_employee(id) # For now I am choosing to not test whether create_employee() works; I'm just using it to create employees.

guess_lists = []
for a in all_authors:
    guess_lists.append(nm.propose_candidates(a))

# TODO: create a repr for Guess and check whether the reprs are the same?



