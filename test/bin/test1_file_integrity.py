# Test that the data structures constructed from file snippets for all following unit tests match what you would get from database queries.

# IMPORTANT! These files have been manually curated!
# If you are creating a new doi_record.txt file, you need to manually remove certain key:value pairs. These are '_id', 'jrc_updated', and 'jrc_inserted'.
# So this:
# "{'_id': ObjectId('669fca86ca18f636c3b03ea2'), 'doi': '10.1007/s12264-024-01253-8', ...
# Becomes this:
# "{'doi': '10.1007/s12264-024-01253-8',  ...
# etc. 
# For '_id', doi_common returns a bson object that gets flattened out into an invalid string in my file. Ditto for the datetime objects for the other two.


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
import jrc_common.jrc_common as JRC
import doi_common.doi_common as doi_common

class TestCase():
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


class DummyArg():
    def __init__(self):
        self.VERBOSE = False
        self.DEBUG = False



# def mimic_jrc_call_people(filename):
#     """
#     Read a file and return a dict or list of dicts that is just what you would get from searching the people system via JRC.call_people_by_id.
#     """
#     with open(filename, 'r') as inF:
#         return(eval(inF.readlines()[0].rstrip('\n')))

def mimic_doi_common_get_doi_record(filename):
    """
    Read a file and return a dict that is just what you would get from doi_common.get_doi_record().
    """
    with open(filename, 'r') as inF:
        return(eval(inF.readlines()[0].rstrip('\n')))

def mimic_doi_common_get_author_details(filename):
    """
    Read a file and return a list that is just what you would get from doi_common.get_author_details().
    """
    with open(filename, 'r') as inF:
        return(eval(inF.readlines()[0].rstrip('\n')))


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


# def parse_out_mongo_key(doi_rec_str):
#     p=re.compile("'_id':\\ ObjectId\\('\w*'\\),\\ ") #  I'm just removing this key:value pair.
#     re.sub(p, "", doi_rec_str[0])
#     return(doi_rec_str)
    


db_connect.initialize_program()
LOGGER = JRC.setup_logging(DummyArg()) 
orcid_collection = db_connect.DB['dis'].orcid
doi_collection = db_connect.DB['dis'].dois

config_file_obj = open('single_author/config.txt', 'r')
config_dict = {line.split(':')[0]: line.split(':')[1].rstrip('\n') for line in config_file_obj.readlines()}
config_file_obj.close()
config = TestCase(**config_dict)

doi_record = mimic_doi_common_get_doi_record(f'{config.dirname}/doi_record.txt')
doic_doi_rec = doi_common.get_doi_record(f'{config.doi}', doi_collection)    
doic_doi_rec.pop('_id') # key definitely exists
doic_doi_rec.pop('jrc_updated', None) # key may not exist
doic_doi_rec.pop('jrc_inserted', None) # key may not exist

author_list = mimic_doi_common_get_author_details(f'{config.dirname}/author_details.txt')
doic_auth_rec = doi_common.get_author_details(doi_record, doi_collection)  #IMPORTANT: NEED TO UPDATE THE SECOND ARG HERE... SOON

compare_doi_records(doi_record, doic_doi_rec)
compare_author_records(author_list, doic_auth_rec)
