# Test that the data structures constructed from file snippets for all following unit tests match what you would get from database queries.

# IMPORTANT! These files have been manually curated!
# If you are creating a new doi_record.txt file, you need to manually remove certain key:value pairs. These are '_id', 'jrc_updated', and 'jrc_inserted'.
# So this:
# "{'_id': ObjectId('669fca86ca18f636c3b03ea2'), 'doi': '10.1007/s12264-024-01253-8', ...
# Becomes this:
# "{'doi': '10.1007/s12264-024-01253-8',  ...
# etc. 
# For '_id', doi_common returns a bson object that gets flattened out into an invalid string in my file. Ditto for the datetime objects for the other two.

# ALSO: for the single author case, I think we will need to add brackets to the file mimicking HHMI People results from name search, basically enclosing it in a list?
# The people system will return a list or a dict, depending on the number of search results.
# In my name match script, I immediately put all the dicts in a list.

# TODO:
# evaluate_candidates
# propose_candidates
# generate_name_permutations

# is_janelian
# set_author_check_attr
# create_author
# create_employee
# create_guess?
# fuzzy_match?

import db_connect
import tc_common
import jrc_common.jrc_common as JRC
import doi_common.doi_common as doi_common

# class TestCase():
#     def __init__(self, **kwargs):
#         for key, value in kwargs.items():
#             setattr(self, key, value)


# def evaluate_file(filename):
#     """
#     Read a file and return a data structure that is just what you would get from a particular doi_common or jrc_common command.
#     """
#     with open(filename, 'r') as inF:
#         return(eval(inF.readlines()[0].rstrip('\n')))


def check_match(message):
    def decorator(func):
        def wrapper(file_data, query_result):
            if func(file_data, query_result):
                print(f"Pass: {message}")
            else:
                print(f"Fail: Mismatch between file and database query: {message}")
        return wrapper
    return decorator

@check_match("DOI record")
def compare_doi_records(file_record, db_record):
    return file_record == db_record

@check_match("Author details record")
def compare_author_records(file_record, db_record):
    return file_record == db_record

@check_match("People system search by ID")
def compare_id_results(file_record, db_record):
    return file_record == db_record



db_connect.initialize_program()
LOGGER = JRC.setup_logging(db_connect.DummyArg()) 
orcid_collection = db_connect.DB['dis'].orcid
doi_collection = db_connect.DB['dis'].dois

config_dict = tc_common.read_config('single_author')
config = tc_common.TestCase(**config_dict)

doi_rec_from_file = tc_common.evaluate_file(f'{config.dirname}/doi_record.txt')
doi_rec_real = doi_common.get_doi_record(f'{config.doi}', doi_collection)    
doi_rec_real.pop('_id') # key definitely exists
doi_rec_real.pop('jrc_updated', None) # key may not exist
doi_rec_real.pop('jrc_inserted', None) # key may not exist

author_list_from_file = tc_common.evaluate_file(f'{config.dirname}/author_details.txt')
author_list_real = doi_common.get_author_details(doi_rec_from_file, doi_collection)  #IMPORTANT: NEED TO UPDATE THE SECOND ARG HERE... SOON

id_results_from_file = [] 
for id in eval(config.initial_candidate_employee_ids):
    id_results_from_file.append( tc_common.evaluate_file(f'{config.dirname}/id_result_{id}.txt') )
id_results_real = [JRC.call_people_by_id(r) for r in eval(config.initial_candidate_employee_ids)]

compare_doi_records(doi_rec_from_file, doi_rec_real)
compare_author_records(author_list_from_file, author_list_real)
compare_id_results(id_results_from_file, id_results_real)
