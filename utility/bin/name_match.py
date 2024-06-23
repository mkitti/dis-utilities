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
import itertools
from termcolor import colored

#TODO: Add some of these to requirements.txt?

################## STUFF YOU SHOULD EDIT ##################
doi = '10.7554/eLife.80660'
##########################################################



class Author:
    def __init__(self, raw_name, orcid=None, affiliations=None, employee_id=None):
        self.raw_name = raw_name
        self.name = self.remove_punctuation(unidecode(raw_name))
        self.orcid = orcid
        self.affiliations = affiliations if affiliations is not None else [] # Need to avoid the python mutable arguments trap
        self.employee_id = employee_id
    
    def remove_punctuation(self, raw_name):
        return(raw_name.translate(str.maketrans('', '', string.punctuation)))


class Employee:
    def __init__(self, id, job_title=None, email=None, first_names=None, middle_names=None, last_names=None):
        self.id = id
        self.job_title = job_title
        self.email = email
        self.first_names = first_names if first_names is not None else [] # Need to avoid the python mutable arguments trap
        self.middle_names = middle_names if middle_names is not None else []
        self.last_names = last_names if last_names is not None else []
    def generate_name_permutations(self):
        #TODO: Check hyphenated lastnames, last names with spaces
        permutations = set()
        # All possible first names + all possible last names
        for first_name, last_name in itertools.product(self.first_names, self.last_names):
            result = f"{first_name} {last_name}"
            permutations.add(result)
        # All possible first names + all possible middle names + all possible last names
        if self.middle_names:
            if any(item for item in self.middle_names if item): # check for ['', '']
                for first_name, middle_name, last_name in itertools.product(self.first_names, self.middle_names, self.last_names):
                    result = f"{first_name} {middle_name} {last_name}"
                    permutations.add(result)
            # All possible first names + all possible middle initials + all possible last names
                for first_name, middle_name, last_name in itertools.product(self.first_names, self.middle_names, self.last_names):
                    middle_initial = middle_name[0]
                    result = f"{first_name} {middle_initial} {last_name}"
                    permutations.add(result)
        return sorted(permutations)

class Guess(Employee):
    def __init__(self, id, job_title=None, email=None, first_names=None, middle_names=None, last_names=None, name=None, score=None):
        super().__init__(id, job_title, email, first_names, middle_names, last_names)
        self.name = name
        self.score = score
    @classmethod
    def from_employee(cls, employee, name=None, score=None):
        return cls(employee.id, employee.job_title, employee.email, employee.first_names, employee.middle_names, employee.last_names, name, score)


class MissingPerson:
    """ This class indicates that searching the HHMI People API yielded no results. """
    pass

class MultipleHits:
    """ This class indicates that an author name matched multiple HHMI employees with equally high scores. """
    def __init__(self, winners=None):
        self.winners = winners if winners is not None else []


people_api_url = "https://hhmipeople-prod.azurewebsites.net/People/"
orcid_api_url = 'https://dis.int.janelia.org/orcid/'
dois_api_url = 'https://dis.int.janelia.org/doi/'
api_key = os.environ.get('PEOPLE_API_KEY')
if not api_key:
    print("Error: Please set the environment variable PEOPLE_API_KEY.")
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
    """ This is needed for GET requests to the DIS dois collection. """
    return( doi.replace("/", "%2F") ) # e.g. 10.1186/s12859-024-05732-7 becomes 10.1186%2Fs12859-024-05732-7


def get_request(url, headers):
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return(response.json())
    else:
        print(f"There was an error with the API GET request. Status code: {response.status_code}.\n Error message: {response.reason}")
        sys.exit(1)

def get_doi_record(doi=doi):
    url = dois_api_url + replace_slashes_in_doi(strip_doi_if_provided_as_url(doi))
    headers = { 'Content-Type': 'application/json' }
    return(get_request(url, headers))

def search_orcid_collection(orcid):
    url = orcid_api_url + orcid
    headers = { 'Content-Type': 'application/json' }
    return(get_request(url, headers))

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
    if not response:
        #print(f"Searching the HHMI People API for {search_term} yielded no results.")
        #sys.exit(1)
        return(MissingPerson())
    else:
        return(response)


# A function to unpack the gnarly data structure we get from Crossref
def create_author_objects(doi_record):
    author_objects = []
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
            author_objects.append(current_author)
    return(author_objects)

def is_janelian(author):
    result = False
    if author.orcid:
        try:
            result = search_orcid_collection(author.orcid)
            result = True
        except Exception as e:
            pass
    if bool(re.search(r'\bJanelia\b', " ".join(author.affiliations))):
        result = True
    return(result)


def create_employee(id):
    idsearch_results = search_people_api(id, 'id')
    if not isinstance(idsearch_results, MissingPerson):
        job_title = job_title = idsearch_results['businessTitle'] if 'businessTitle' in idsearch_results else None
        email = idsearch_results['email'] if 'email' in idsearch_results else None
        first_names = [ idsearch_results['nameFirstPreferred'], idsearch_results['nameFirst'] ]
        middle_names = [ idsearch_results['nameMiddlePreferred'], idsearch_results['nameMiddle'] ]
        last_names = [ idsearch_results['nameLastPreferred'], idsearch_results['nameLast'] ]
        return(
            Employee(
            id,
            job_title=job_title,
            email=email,
            first_names=first_names, 
            middle_names=middle_names, 
            last_names=last_names)
        )
    else:
        return(MissingPerson())


def guess_employee(author):
    candidate_employees = [] # Includes false positives. For example, if I search 'Virginia',
    # both Virginia Scarlett and Virginia Ruetten will be in this list.
    search_term  = max(author.name.split(), key=len) # We can only search the People API by one name, so just pick the longest one
    namesearch_results = search_people_api(search_term, 'name')
    if not isinstance(namesearch_results, MissingPerson):
        candidate_employee_ids = [ employee_dic['employeeId'] for employee_dic in namesearch_results ]
        for id in candidate_employee_ids:
            employee_from_id_search = create_employee(id) 
            if not isinstance(employee_from_id_search, MissingPerson):
                candidate_employees.append(employee_from_id_search)
            else:
                return(MissingPerson)
        guesses = []
        for employee in candidate_employees:
            employee_permuted_names = employee.generate_name_permutations()
            for name in employee_permuted_names:
                guesses.append(Guess.from_employee(employee, name=name)) # Each employee will generate several guesses, e.g. Virginia T Scarlett, Virginia Scarlett
        for guess in guesses:
            guess.score = fuzz.token_sort_ratio(author.name.lower(), guess.name.lower())
        high_score = max( [g.score for g in guesses] )
        winner = [ g for g in guesses if g.score == high_score ]
        if len(winner) > 1:
            return(MultipleHits(winners = winner))
        else:
            return(winner[0])
    else:
        return(MissingPerson())








# def guess_employee(author):
#     employees_from_api_search = [] # Includes false positives. For example, if I search 'Virginia',
#     # both Virginia Scarlett and Virginia Ruetten will be in employees_from_api_search.
#     search_term  = max(author.name.split(), key=len) # We can only search the People API by one name, so just pick the longest one
#     namesearch_results = search_people_api(search_term, 'name')
#     if not isinstance(namesearch_results, MissingPerson):
#         candidate_employee_ids = [ employee_dic['employeeId'] for employee_dic in namesearch_results ]
#         for id in candidate_employee_ids:
#             idsearch_results = create_employee(id)
#             if not isinstance(idsearch_results, MissingPerson):
#                 employees_from_api_search.append(create_employee(id))
#         permuted_names = OrderedDict()
#         for employee in employees_from_api_search:
#             employee_permuted_names = employee.generate_name_permutations()
#             for name in employee_permuted_names:
#                 permuted_names[name] = employee.id
#         fuzzy_match_scores = []
#         permuted_names_list = permuted_names.keys()
#         for permuted_name in permuted_names_list:
#             fuzzy_match_scores.append( fuzz.token_sort_ratio(author.name.lower(), permuted_name.lower()) )
#         #Get the index of the highest score, and print a warning if
#         #there are multiple maximum values, e.g., if someone is in the People database twice
#         # TODO: Maybe change this to use Guess objects instead of relying so much on list indices?
#         winner_index_list = [ i for i, value in enumerate(fuzzy_match_scores) if value == max(fuzzy_match_scores) ]
#         winner_index = winner_index_list[0]
#         if len(winner_index_list) > 1:
#             print("Multiple high scoring matches found:")
#             print([permuted_names_list[i] for i in winner_index_list]) 
#             print("Choosing the first one.")
#         return(
#             Guess.from_employee(
#                 employees_from_api_search[winner_index], 
#                 permuted_names_list[winner_index],
#                 fuzzy_match_scores[winner_index])
#             )
#     else:
#         return(MissingPerson())






# guess_employee(janelian_authors[2])

# ------------------------------------------------------------------------


if __name__ == '__main__':
    doi_record = get_doi_record()
    all_authors = create_author_objects(doi_record)
    janelian_authors = [ a for a in all_authors if is_janelian(a) ]
    for author in janelian_authors:
        if author.orcid:
            orcid_record = search_orcid_collection(author.orcid) # I am searching the ORCID collection twice:
            # once to check whether they are Janelian, and later to get their employee ID. This could be 
            # optimized for efficiency.
            if 'employeeId' in orcid_record['data'][0]:
                print(f"{author.name} is in our ORCID collection, and has an employee ID: ORCID:{author.orcid}, employee ID: {orcid_record['data'][0]['employeeId']}") 
            else:
                print(f'{author.name} is in our ORCID collection, but does not have an employee ID.')
                best_guess = guess_employee(author)
                if type(best_guess) is MissingPerson:
                    print(f"{author.name} could not be found in the HHMI People API.")
                else:
                    print(f"{author.name}: Employee best guess: {best_guess.name}, ID: {best_guess.id}, job title: {best_guess.job_title}, email: {best_guess.email}, Confidence: {round(best_guess.score, ndigits = 3)}")
        else:
            print(f'{author.name} does not have an ORCID.')
            best_guess = guess_employee(author)
            if isinstance(best_guess, MissingPerson):
                print(f"{author.name} could not be found in the HHMI People API.")
            elif isinstance(best_guess, MultipleHits):            
                print("Multiple high scoring matches found:")
                for guess in best_guess.winners:
                    print(colored(f"{guess.name}, {guess.job_title}, {guess.email}", 'blue'))
                print(colored(f"Recommended action: browse above list for matches to {author.name}.", 'red', 'on_yellow'))
            else:
                print(f"{author.name}: Employee best guess: {best_guess.name}, ID: {best_guess.id}, job title: {best_guess.job_title}, email: {best_guess.email}, Confidence: {round(best_guess.score, ndigits = 3)}")
                if float(best_guess.score) > 85.0:
                    print(colored(f"Recommended action: add employee ID {best_guess.id} to {author.name} for this doi.", 'red', 'on_yellow'))
        print('\n')






    

    
                


















    


















"""


SCRAPS; OLD




# Order of first/last name doesn't matter
def match_two_names(name1, name2):
    name1_dec, name2_dec = unidecode(name1), unidecode(name2)
    name1_no_punc, name2_no_punc = name1_dec.translate(str.maketrans('', '', string.punctuation)), name2_dec.translate(str.maketrans('', '', string.punctuation))
    return(
        fuzz.token_sort_ratio(name1_no_punc.lower(), name2_no_punc.lower())
        )



# #Checks whether the search yielded multiple records, and if so, returns the highest-scoring record.
# def filter_namesearch_results(raw_search_results, author_name):
#     if len(raw_search_results) > 1:
#         resulting_names = [ concatenate_name_parts(r) for r in raw_search_results ]
#         scores = []
#         for result_name in resulting_names:
#             score = match_two_names(author_name, result_name)
#             scores.append(score)
#             print(f"{author_name} vs. {result_name}: {round(score, ndigits = 3)}")
#         #Get the index of the highest score, and print a warning if
#         #there are multiple maximum values, e.g., if someone is in the People database twice
#         max_indices = [i for i, value in enumerate(scores) if value == max(scores)]
#         if len(max_indices) > 1:
#             print("Multiple high scoring matches found:")
#             print([resulting_names[i] for i in max_indices]) #TODO: Add their businessTitle and email in case there are two people who actually have the same name
#             print("Choosing the first one.")
#             return(raw_search_results[max_indices[0]])
#         else:
#             index_of_highest_match = max_indices[0]
#             print(f"Choosing {resulting_names[index_of_highest_match]}")
#             return(raw_search_results[index_of_highest_match])
#     else:
#         return(raw_search_results[0])





#Strategy: 
# Decode to ascii, Remove punctuation, and make lower case


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



raw_search_results = search_people_api_by_name(search_term)
chosen_record = filter_search_results(raw_search_results)

# Example author record, for debugging:
 {'ORCID': 'http://orcid.org/0000-0003-0369-9788', 
 'affiliation': [
     {'id': [{'asserted-by': 'publisher', 'id': 'https://ror.org/013sk6x84', 'id-type': 'ROR'}], 
      'name': 'Janelia Research Campus, Howard Hughes Medical Institute', 
      'place': ['Ashburn, United States']}
      ], 
      'authenticated-orcid': True, 
      'family': 'Meissner', 
      'given': 'Geoffrey W', 
      'sequence': 'first'}


# handy for debugging: [i for i in enumerate([' '.join((e['nameFirstPreferred'], e['nameLastPreferred'])) for e in raw_search_results])]



"""
