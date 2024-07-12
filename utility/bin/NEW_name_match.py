"""
New version of name_match.py that will connect to the DIS database directly instead of making REST requests.
"""

import os
import sys
from rapidfuzz import fuzz
import string
from unidecode import unidecode
import re
import itertools
from termcolor import colored
import jrc_common.jrc_common as JRC
import doi_common.doi_common as doi_common
from operator import attrgetter
import sys
import inquirer
from inquirer.themes import BlueComposure

#TODO: Add some of these to requirements.txt?

################## STUFF YOU SHOULD EDIT ##################
doi = '10.7554/eLife.80660'
##########################################################

sys.path.append('~/dis-utilities/sync/bin')
api_key = os.environ.get('PEOPLE_API_KEY')
if not api_key:
    print("Error: Please set the environment variable PEOPLE_API_KEY.")
    sys.exit(1)

DB = {}
PROJECT = {}

def initialize_program():
    ''' Intialize the program
        Keyword arguments:
          None
        Returns:
          None
    '''
    # Database
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err:
        terminate_program(err)
    dbs = ['dis']
    for source in dbs:
        manifold = 'prod'
        dbo = attrgetter(f"{source}.{manifold}.write")(dbconfig)
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)
    try:
        rows = DB['dis'].project_map.find({})
    except Exception as err:
        terminate_program(err)
    for row in rows:
        PROJECT[row['name']] = row['project']

def terminate_program(msg=None):
    ''' Terminate the program gracefully
        Keyword arguments:
          msg: error message
        Returns:
          None
    '''
    if msg:
        print(msg)
        sys.exit(-1)
    else:
        sys.exit(0)
#TODO: 
# if msg:
#         if not isinstance(msg, str):
#             msg = f"An exception of type {type(msg).__name__} occurred. Arguments:\n{msg.args}"
#         LOGGER.critical(msg)
#     sys.exit(-1 if msg else 0)

class Author:
    """ Author objects are constructed solely from the CrossRef-provided author information. """
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



def search_people_api(search_term, mode):
    response = None
    if mode not in {'name', 'id'}:
        raise ValueError("HHMI People API search mode must be either 'name' or 'id'.")
    if mode == 'name':
        response = JRC.call_people_by_name(search_term)
    elif mode == 'id':
        response = JRC.call_people_by_id(search_term)
    if not response:
        return(MissingPerson())
    else:
        return(response)


def search_orcid_collection(orcid, collection):
    return(
        doi_common.single_orcid_lookup(orcid, collection, 'orcid')
        )

def get_doi_record(doi):
    result = JRC.call_crossref(doi)
    return( result['message'] )

def strip_orcid_if_provided_as_url(orcid):
    prefixes = ["http://orcid.org/", "https://orcid.org/"]
    for prefix in prefixes:
        if orcid.startswith(prefix):
            return orcid[len(prefix):]
    return(orcid)

def create_author_objects(doi_record):
    author_objects = []
    for author_record in doi_record['author']:
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

def evaluate_guess(author, best_guess, success_message):
    if isinstance(best_guess, MissingPerson):
        print(f"{author.name} could not be found in the HHMI People API.")
        return(False)
    if isinstance(best_guess, MultipleHits):
        print("Multiple high scoring matches found:")
        for guess in best_guess.winners:
            print(colored(f"{guess.name}, {guess.job_title}, {guess.email}", 'blue'))
        quest = [inquirer.Checkbox('decision', carousel=True, message="Choose an option", choices=[guess.name for guess in best_guess.winners] + ['None of the above'], default=['None of the above'])]
        ans = inquirer.prompt(quest, theme=BlueComposure())
        if ans['decision'] != ['None of the above']:
            quest = [inquirer.List('action',message = success_message.substitute(name=best_guess.name),choices = ['Yes', 'No'])]
            ans = inquirer.prompt(quest, theme=BlueComposure())
            if ans['action'] == 'Yes':
                return(True)
            else:
                return(False)
        else:
            return(False)
    else:
        if float(best_guess.score) < 85.0:
            print(
                f"Employee best guess: {best_guess.name}, ID: {best_guess.id}, job title: {best_guess.job_title}, email: {best_guess.email}, Confidence: {round(best_guess.score, ndigits = 3)}"
                )
        else:
            print(colored(
                f"Employee best guess: {best_guess.name}, ID: {best_guess.id}, job title: {best_guess.job_title}, email: {best_guess.email}, Confidence: {round(best_guess.score, ndigits = 3)}",
                "blue"
                ))
            quest = [inquirer.List('decision', message=f"Select {best_guess.name}?", choices=['Yes', 'No'])]
            ans = inquirer.prompt(quest, theme=BlueComposure())
            if ans['decision'] == 'Yes':
                quest = [ inquirer.List('action', message = success_message.substitute(name=best_guess.name), choices = ['Yes', 'No']) ]
                ans = inquirer.prompt(quest, theme=BlueComposure())
                if ans['action'] == 'Yes':
                    return(True)
                else:
                    return(False)
            else:
                return(False)


def add_employeeId_to_orcid_record(orcid, employee_id, collection):
    return(
        doi_common.update_existing_orcid(lookup=orcid, add=employee_id, coll=collection, lookup_by='orcid')
        )


# -----------------------------------------------------------------------------

if __name__ == '__main__':
    initialize_program()
    orcid_collection = DB['dis'].orcid
    doi_record = get_doi_record(doi)
    all_authors = create_author_objects(doi_record)
    janelian_authors = [ a for a in all_authors if is_janelian(a) ]
    for author in janelian_authors:
        print() # whitespace
        if author.orcid:
            mongo_orcid_record = search_orcid_collection(author.orcid, orcid_collection)
            if mongo_orcid_record:
                if 'employeeId' in mongo_orcid_record:
                    print( f"{author.name} is in our ORCID collection, with both an ORCID an employee ID." )
                    # Do nothing
                elif 'employeeId' not in mongo_orcid_record:
                    print( f"{author.name} is in our ORCID collection, but without an employee ID." )
                    best_guess = guess_employee(author)
                    proceed = evaluate_guess(author, best_guess, string.Template("Confirm you wish to add $name's employee ID to existing ORCID record")) #can't use best_guess.name bc guess may be None (MissingPerson)
                    if proceed:
                        add_employeeId_to_orcid_record(author.orcid, best_guess.id, orcid_collection)
            elif not mongo_orcid_record:
                print( f"{author.name} has an ORCID on this paper, but this ORCID is not in our collection." )
                best_guess = guess_employee(author)
                proceed = evaluate_guess(author, best_guess, string.Template("Confirm you wish to create an ORCID record for $name, with both their employee ID and their ORCID"))
                if proceed:

        elif not author.orcid:
            print( f"{author.name} does not have an ORCID on this paper." )
            best_guess = guess_employee(author)
            evaluate_guess(author, best_guess, string.Template("Confirm you wish to create an ORCID record for $name with an employee ID only"))







#For bug testing, do not run!!!
# import NEW_name_match as nm
# nm.initialize_program()
# orcid_collection = nm.DB['dis'].orcid
# doi_record = nm.get_doi_record('10.7554/eLife.80660')
# all_authors = nm.create_author_objects(doi_record)
# janelian_authors = [ a for a in all_authors if nm.is_janelian(a) ]
# for author in janelian_authors:
#     print() # whitespace
#     if author.orcid:
#         mongo_orcid_record = nm.search_orcid_collection(author.orcid, orcid_collection)
#         if mongo_orcid_record:
#             if 'employeeId' in mongo_orcid_record:
#                 print( f"{author.name} is in our ORCID collection, and the ORCID record has an employee ID." )
#                 # Do nothing
#             elif 'employeeId' not in mongo_orcid_record:
#                 print( f"{author.name} is in our ORCID collection, but without an employee ID." )
#                 best_guess = nm.guess_employee(author)
#                 nm.evaluate_guess(author, best_guess, nm.string.Template("Confirm you wish to add $name's employee ID to existing ORCID record")) #can't use regular string formatting bc guess may be None (MissingPerson)
#         elif not mongo_orcid_record:
#             print( f"{author.name} has an ORCID on this paper, but this ORCID is not in our collection." )
#             best_guess = nm.guess_employee(author)
#             nm.evaluate_guess(author, best_guess, nm.string.Template("Confirm you wish to create an ORCID record for $name, with both their employee ID and their ORCID"))
#     elif not author.orcid:
#         print( f"{author.name} does not have an ORCID on this paper." )
#         best_guess = nm.guess_employee(author)
#         nm.evaluate_guess(author, best_guess, nm.string.Template("Confirm you wish to create an ORCID record for $name with an employee ID only"))