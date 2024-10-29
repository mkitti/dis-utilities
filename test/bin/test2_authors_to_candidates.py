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
config_dict = tc_common.read_config('single_author')
config = tc_common.TestCase(**config_dict)


doi_rec_from_file = config.doi_record()
author_list_from_file = config.author_details()
all_authors = [ nm.create_author(author_record) for author_record in author_list_from_file]

ids = []
for a in all_authors:
    name = nm.HumanName(a.name)
    ids = nm.name_search(name.first, name.last)


if ids == config.candidate_ids():
    print('Pass: initial candidate employee IDs')
else:
    print(f'Fail: initial candidate employee IDs\nExpected:{config.candidate_ids()}\nReturned:{ids}')


guess_lists = []
for a in all_authors:
    guess_lists.append(nm.propose_candidates(a))

target = config.parse_proposed_guesses() # Guess objects from file

for i in range(len(guess_lists)):
    for i2 in range(len(guess_lists[i])):
        if target[i][i2] != str(repr(guess_lists[i][i2])):
            print(f'Fail: initial proposed guesses\nExpected:{target}\nReturned:{guess_lists}') #guess_lists items won't have double-quotes when you print them because they're reprs
            sys.exit(0)

    print('Pass: initial proposed guesses')






