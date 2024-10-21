# before running, put utility/bin in path like so:
# export PYTHONPATH="${PYTHONPATH}:/groups/scicompsoft/home/scarlettv/dis-utilities/utility/bin"

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

import name_match as nm

nm.initialize_program()
orcid_collection = nm.DB['dis'].orcid
doi_collection = nm.DB['dis'].dois

def mimic_jrc_call_people(filename):
    """
    Read a file and return a dict that is just what you would get from searching the people system via JRC.call_people_by_id.
    Assumes the file contains the direct results of either nm.JRC.call_people_by_id('someId') or nm.JRC.call_people_by_name('someName').
    """
    with open(filename, 'r') as inF:
        return(eval(inF.readlines()[0].rstrip('\n')))

def mimic_doi_common_get_author_details

res = mimic_jrc_call_people('employee_middle_name_none.txt')
#jrc_res = nm.JRC.call_people_by_id('42651')
#res==jrc_res Should be True
res2 = mimic_jrc_call_people('employee_multiple_name_hits.txt')
#jrc_res2 = nm.JRC.call_people_by_name('Miguel')
#res2==jrc_res2 Should be True
res3 = mimic_jrc_call_people('employee_used_to_be_in_system_twice.txt')
#jrc_res3 = nm.JRC.call_people_by_name('Clapham')
#res3==jrc_res3 Should be True

