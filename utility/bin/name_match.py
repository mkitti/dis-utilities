"""

Goal: identify which article authors correspond to which employee IDs.
First, try to find an employee id in our ORCID collection. 
If the author does not have an employee id in our ORCID collection, then we 
must use fuzzy string matching to make a 'best guess' at the closest employee name. 
We do this by creating all reasonable permutations of all employee names, 
and matching the author name against all possible employee names.
The script recommends employee ids for authors where an 85% match or higher was found.

"""

#TODO: Use https://dis.int.janelia.org/doi/janelians/10.7554/eLife.80660 to identify alumni


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
            permutations.add(
                f"{first_name} {last_name}"
            )
        # All possible first names + all possible middle names + all possible last names
        if self.middle_names:
            if any(item for item in self.middle_names if item): # check for ['', '']
                for first_name, middle_name, last_name in itertools.product(self.first_names, self.middle_names, self.last_names):
                    permutations.add(
                        f"{first_name} {middle_name} {last_name}"
                    )
            # All possible first names + all possible middle initials + all possible last names
                for first_name, middle_name, last_name in itertools.product(self.first_names, self.middle_names, self.last_names):
                    middle_initial = middle_name[0]
                    permutations.add(
                        f"{first_name} {middle_initial} {last_name}"
                    )
        return sorted(permutations)

class Guess(Employee):
    def __init__(self, id, job_title=None, email=None, first_names=None, middle_names=None, last_names=None, name=None, score=None):
        super().__init__(id, job_title, email, first_names, middle_names, last_names)
        self.name = name
        self.score = score


def instantiate_guess(employee, name=None, score=None):
    return(Guess(
        employee.id, 
        employee.job_title, 
        employee.email, 
        employee.first_names, 
        employee.middle_names, 
        employee.last_names, 
        name, 
        score
        )
    )


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
                guesses.append(instantiate_guess(employee, name=name)) # Each employee will generate several guesses, e.g. Virginia T Scarlett, Virginia Scarlett
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
            print(f'Author {author.name} does not have an ORCID on this paper.')
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





