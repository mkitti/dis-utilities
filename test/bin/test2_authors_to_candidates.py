# before running, put utility/bin in path like so:
# export PYTHONPATH="${PYTHONPATH}:/groups/scicompsoft/home/scarlettv/dis-utilities/utility/bin"

# VERY IMPORTANT: Expects the list of lists of guesses in your config file to use unusual separators, because brackets are not eval-safe.
# sublists should be separated with ';', and strings should be separated with '|'
# so:
# [ ['hats', 'scarves'], ['boots', 'gloves'], ['earmuffs'] ]
# must be written in the config file like this:
# hats|scarves;boots|gloves;earmuffs

import db_connect
import tc_common
import jrc_common.jrc_common as JRC
import name_match as nm
import sys



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


if ids == eval(config.initial_candidate_employee_ids):
    print('Pass: initial candidate employee IDs')
else:
    print(f'Fail: initial candidate employee IDs\nExpected:{eval(config.initial_candidate_employee_ids)}\nReturned:{ids}')

# nm_employees = []
# for id in ids:
#     nm.create_employee(id) # For now I am choosing to not test whether create_employee() works; I'm just using it to create employees.

guess_lists = []
for a in all_authors:
    guess_lists.append(nm.propose_candidates(a))

target = [s.split('|') for s in config.proposed_guesses.split(';')]

for i in range(len(guess_lists)):
    for i2 in range(len(guess_lists[i])):
        if target[i][i2] != str(repr(guess_lists[i][i2])):
            print(f'Fail: initial proposed guesses\nExpected:{target}\nReturned:{guess_lists}') #guess_lists items won't have double-quotes when you print them because they're reprs
            sys.exit(0)

    print('Pass: initial proposed guesses')






