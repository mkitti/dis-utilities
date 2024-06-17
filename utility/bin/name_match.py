"""

Goal: identify which article authors correspond to which employee IDs.
We will only consider authors who:
1) Have Janelia listed in their affiliations in the DOI metadata, OR
2) Are in our ORCID collection.
The second case is easy. If they are in the ORCID collection, 
then there will be an employeeId property, and we're done.
In the first case, we still check if they have an ORCID that simply isn't in our collection.
In this case, we want to print a message that informs the user of this.
If the author does not have an ORCID, or they do have an ORCID but it's not in our ORCID collection, 
then we must use fuzzy string matching to make a 'best guess' at the closest employee name. 
We'll do this by creating all reasonable permutations of all employee names, 
and matching the author name against all possible employee names.

"""

import requests
import os
import sys
from rapidfuzz import fuzz
import string
from unidecode import unidecode
import re
#TODO: Add some of these to requirements.txt?

################## STUFF YOU SHOULD EDIT ##################
doi = '10.7554/eLife.80660'
##########################################################



class Author:
    def __init__(self, raw_name, orcid=None, affiliations=None):
        self.raw_name = raw_name
        self.name = self.remove_punctuation(unidecode(raw_name))
        self.orcid = orcid
        self.affiliations = affiliations if affiliations is not None else []
    
    def remove_punctuation(self, raw_name):
        return(raw_name.translate(str.maketrans('', '', string.punctuation)))


class Employee:
    def __init__(self, **kwargs):
        self.HHMI_data = kwargs
        self.names = []
    
    def add_HHMI_data(self, additional_dict):
        self.HHMI_data.update(additional_dict)
    
    def extract_names(self): #TODO: put some of the concatenate names functionality into here.
        # The idea is to create a list of all possible names for a given employee.
        # In the end, we will compare the author name to a list of employee names, where a particular employee
        # may be present in the list multiple times under multiple names. 
        self.names = [
            self.HHMI_data.get('nameFirstPreferred', ''),
            self.HHMI_data.get('nameMiddlePreferred', ''),
            self.HHMI_data.get('nameLastPreferred', '')
        ]

# Example dictionary
# employee_data = {
#     "name": "John Doe",
#     "position": "Research Scientist",
#     "department": "Biology"
# }

# Creating an instance of Employee using the dictionary
# employee = Employee(**employee_data)
# print(employee.HHMI_data)
# Output: {'name': 'John Doe', 'position': 'Research Scientist', 'department': 'Biology'}



people_api_url = "https://hhmipeople-prod.azurewebsites.net/People/"
orcid_api_url = 'https://dis.int.janelia.org/orcid/'
dois_api_url = 'https://dis.int.janelia.org/doi/'
#search_term  = max(author_name.split(), key=len) # We can only search the People API by one name, so just pick the longest one
api_key = os.environ.get('PEOPLE_API_KEY')
if not api_key:
    print("Error: Please set the environment variable PEOPLE_API_KEY.")
    sys.exit(1)



def get_request(url, headers):
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return(response.json())
    else:
        print(f"There was an error with the API GET request. Status code: {response.status_code}.\n Error message: {response.reason}")
        sys.exit(1)

def strip_orcid_if_provided_as_url(orcid):
    prefixes = ["http://orcid.org/", "https://orcid.org/"]
    for prefix in prefixes:
        if orcid.startswith(prefix):
            return orcid[len(prefix):]
    return(orcid)

def strip_doi_if_provided_as_url(doi=doi, substring=".org/10.", doi_index_in_substring = 5):
    # Find all occurrences of the substring
    occurrences = [i for i in range(len(doi)) if doi.startswith(substring, i)]
    if len(occurrences) > 1:
        print("Warning: Please check that your DOI is formatted correctly.")
        exit(1)  # Exit with a warning code
    elif len(occurrences) == 1:
        doi_index_in_string = occurrences[0]
        stripped_doi = doi[doi_index_in_string + doi_index_in_substring:]
        return(stripped_doi)
    else:
        return(doi)

def replace_slashes_in_doi(doi=doi):
    return( doi.replace("/", "%2F") ) # e.g. 10.1186/s12859-024-05732-7 becomes 10.1186%2Fs12859-024-05732-7



def get_doi_record(doi=doi):
    url = dois_api_url + replace_slashes_in_doi(strip_doi_if_provided_as_url(doi))
    headers = { 'Content-Type': 'application/json' }
    return(get_request(url, headers))

def search_orcid_collection(orcid):
    url = orcid_api_url + orcid
    headers = { 'Content-Type': 'application/json' }
    return(get_request(url, headers))


# Example author record:
""" {'ORCID': 'http://orcid.org/0000-0003-0369-9788', 
 'affiliation': [
     {'id': [{'asserted-by': 'publisher', 'id': 'https://ror.org/013sk6x84', 'id-type': 'ROR'}], 
      'name': 'Janelia Research Campus, Howard Hughes Medical Institute', 
      'place': ['Ashburn, United States']}
      ], 
      'authenticated-orcid': True, 
      'family': 'Meissner', 
      'given': 'Geoffrey W', 
      'sequence': 'first'} """


def create_author_objsNEW(doi_record):
    author_objs = []
    for author_record in doi_record['data']['author']:
        if 'given' in author_record and 'family' in author_record:
            full_name = ' '.join((author_record['given'], author_record['family']))
            current_author = Author( full_name )
            if 'affiliation' in author_record and author_record['affiliation']:
                for affiliation in author_record['affiliation']:
                    if 'name' in affiliation:
                        current_author.affiliations.append(affiliation['name'])
            if 'ORCID' in author_record:
                current_author.orcid = strip_orcid_if_provided_as_url(author_record['ORCID'])
            author_objs.append(current_author)
    return(author_objs)

def is_janelian(author_obj):
    result = False
    if author_obj.orcid:
        try:
            result = search_orcid_collection(author.orcid)
            result = True
        except Exception as e:
            pass
    if bool(re.search(r'\bJanelia\b', " ".join(author_obj.affiliations))):
        result = True
    return(result)
    
# ------------------------------------------------------------------------

doi_record = get_doi_record()
all_authors = create_author_objsNEW(doi_record)
janelian_authors = [ a for a in all_authors if is_janelian(a) ]
for a in janelian_authors:
    print(a.name)


    




















"""
def search_people_api(search_term, mode):
    if mode not in {'name', 'id'}:
        raise ValueError("HHMI People API search mode must be either 'name' or 'id'.")
    url = ''
    if mode == 'name':
        url = people_api_url + 'Search/ByName/' + search_term
    elif mode == 'id':
        url = people_api_url + 'Person/GetById/' + search_term
    headers = { 'APIKey': f'{api_key}', 'Content-Type': 'application/json' }
    response = get_request(url, headers)
    if not response.json():
        print("Your search of the HHMI People API yielded no results.")
        sys.exit(1)
    else:
        return(response.json())





#TODO: Maybe mash up their preferred names and legal names, e.g. to match James Liu <-> Zhe J. Liu?
#TODO: decode from unicode to ascii to properly handle accented characters use unidecode https://pypi.org/project/Unidecode/
#TODO: Check hyphenated lastnames
#Strategy: 
# Decode to ascii, Remove punctuation, and make lower case
# Order of first/last name doesn't matter
def match_two_names(name1, name2):
    name1_dec, name2_dec = unidecode(name1), unidecode(name2)
    name1_no_punc, name2_no_punc = name1_dec.translate(str.maketrans('', '', string.punctuation)), name2_dec.translate(str.maketrans('', '', string.punctuation))
    return(
        fuzz.token_sort_ratio(name1_no_punc.lower(), name2_no_punc.lower())
        )

# If the author name is three words and the middle word is a single letter, AND they have a middle name in HHMI People, 
# then include the middle initial from HHMI People in their name.
def concatenate_name_parts(person_record, author_name_np = author_name_np):
    firstname_lastname = ' '.join((person_record['nameFirstPreferred'], person_record['nameLastPreferred']))
    if len(author_name_np.split(' ')) == 3:
        if len(author_name_np.split(' ')[1]) == 1:
            if person_record['nameMiddlePreferred']: # Note that if the person doesn't have a middle name, this field may randomly be either None or ''.
                return( ' '.join((person_record['nameFirstPreferred'], person_record['nameMiddlePreferred'][0], person_record['nameLastPreferred'])) )
            else:
                return( firstname_lastname )
        else:
            return( firstname_lastname )
    else:
        return( firstname_lastname )


#Checks whether the search yielded multiple records, and if so, returns the highest-scoring record.
def filter_search_results(raw_search_results, author_name_np = author_name_np):
    if len(raw_search_results) > 1:
        resulting_names = [ concatenate_name_parts(r) for r in raw_search_results ]
        scores = []
        for result_name in resulting_names:
            score = match_two_names(author_name_np, result_name)
            scores.append(score)
            print(f"{author_name_np} vs. {result_name}: {round(score, ndigits = 3)}")
        #Get the index of the highest score, and print a warning if
        #there are multiple maximum values, e.g., if someone is in the People database twice
        max_indices = [i for i, value in enumerate(scores) if value == max(scores)]
        if len(max_indices) > 1:
            print("Multiple high scoring matches found:")
            print([resulting_names[i] for i in max_indices]) #TODO: Add their businessTitle and email in case there are two people who actually have the same name
            print("Choosing the first one.")
            return(raw_search_results[max_indices[0]])
        else:
            index_of_highest_match = max_indices[0]
            print(f"Choosing {resulting_names[index_of_highest_match]}")
            return(raw_search_results[index_of_highest_match])
    else:
        return(raw_search_results[0])

raw_search_results = search_people_api_by_name(search_term)
chosen_record = filter_search_results(raw_search_results)


# handy for debugging: [i for i in enumerate([' '.join((e['nameFirstPreferred'], e['nameLastPreferred'])) for e in raw_search_results])]



"""
