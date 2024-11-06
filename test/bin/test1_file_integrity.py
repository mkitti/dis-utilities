# Test that the data structures constructed from file snippets for all following unit tests match what you would get from database queries.
# Run like so:
# python3 test1_file_integrity.py <dir_name>
# python3 test1_file_integrity.py single_author

import db_connect
import tc_common
import jrc_common.jrc_common as JRC
import doi_common.doi_common as doi_common
import sys

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


# Boilerplate: initialize DB connection
db_connect.initialize_program()
LOGGER = JRC.setup_logging(db_connect.DummyArg()) 
orcid_collection = db_connect.DB['dis'].orcid
doi_collection = db_connect.DB['dis'].dois

#Boilerplate: create a TestCase object (attributes come from config file)
config = tc_common.TestCase()
config.read_config(sys.argv[1])


#doi_rec_from_file = config.doi_record()
doi_rec_from_file = config.doi_record()
doi_rec_real = doi_common.get_doi_record(f'{config.doi}', doi_collection)


author_list_from_file = config.author_details()
author_list_real = doi_common.get_author_details(doi_rec_real, doi_collection)  #IMPORTANT: NEED TO UPDATE THE SECOND ARG HERE... SOON

id_results_from_file = config.id_result()
id_results_real = [JRC.call_people_by_id(r) for r in config.candidate_ids()]

compare_doi_records(doi_rec_from_file, str(doi_rec_real))
compare_author_records(author_list_from_file, str(author_list_real))
compare_id_results(id_results_from_file, [str(r) for r in id_results_real])
