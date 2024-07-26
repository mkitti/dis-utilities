"""
Given a DOI, try to identify Janelia authors who don't have ORCIDs and correlate them with employees.
Update ORCID records as needed.
"""

import os
import sys
from rapidfuzz import fuzz, utils
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
import argparse

#TODO: Add some of these imports to requirements.txt?
#TODO: Handle names that CrossRef butchered, e.g. 'Miguel Angel NunezOchoa' for 10.1101/2024.06.30.601394, which can't be found in the People API.
#TODO: after running this script, run update_dois.py to add someone to jrc_authors
#TODO: Filter out People search results where location != Janelia
#BUG! The menu to select authors manually doesn't show all authors for 10.1101/2024.05.09.593460
#BUG! The ORCID records I create don't contain family or given names. Example: Briana Yarbrough, employeeId 54017


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
    """ Employees are constructed from information found in the HHMI People database. """
    def __init__(self, id, job_title=None, email=None, first_names=None, middle_names=None, last_names=None):
        self.id = id
        self.job_title = job_title
        self.email = email
        self.first_names = list(set(first_names)) if first_names is not None else [] # Need to avoid the python mutable arguments trap
        self.middle_names = list(set(middle_names)) if middle_names is not None else []
        self.last_names = list(set(last_names)) if last_names is not None else []
    def generate_name_permutations(self):
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
        return list(sorted(permutations))
    def generate_family_name_permutations(self): #for adding records to the ORCID collection
        permutations = set()
        all_middle_names = [n for n in self.middle_names if n not in (None, '')]
        if all_middle_names:
            for first in self.first_names:
                for middle in all_middle_names:
                    permutations.add(f"{first} {middle}")
                    permutations.add(f"{first} {middle[0]}.")
            return list(sorted(permutations))
        else:
            return list(set(self.first_names))

class Guess(Employee):
    """ A Guess is a subtype of Employee that includes just one name permutation 
    (e.g. Gerald W Rubin) and a fuzzy match score (calculated before the guess object is instantiated). """
    def __init__(self, id, job_title=None, email=None, first_names=None, middle_names=None, last_names=None, name=None, score=None):
        super().__init__(id, job_title, email, first_names, middle_names, last_names)
        self.name = name
        self.score = score

class MissingPerson:
    """ This class indicates that searching the HHMI People API yielded no results. """
    pass

class MultipleHits:
    """ 
    This class indicates that an author name matched multiple HHMI employees with equally high scores. 
    This class's only attribute is a list of employee objects.
    """
    def __init__(self, winners=None):
        self.winners = winners if winners is not None else []



### Functions for instantiating objects of my custom classes

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

def create_guess(employee, name=None, score=None):
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



### Functions for matching authors to employees

def guess_employee(author):
    """ 
    Given an author object, search the People API for one or more matches using the People Search. 
    Arguments: 
        author: an author object.
    Returns:
        A missing person object, a multiple hits object, OR an employee object.
    """
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
                guesses.append(create_guess(employee, name=name)) # Each employee will generate several guesses, e.g. Virginia T Scarlett, Virginia Scarlett
        for guess in guesses:
            guess.score = fuzz.token_sort_ratio(author.name, guess.name, processor=utils.default_process) #processor will convert the strings to lowercase, remove non-alphanumeric characters, and trim whitespace 
        high_score = max( [g.score for g in guesses] )
        winner = [ g for g in guesses if g.score == high_score ]
        if len(winner) > 1:
            return(MultipleHits(winners = winner))
        else:
            return(winner[0])
    else:
        return(MissingPerson())

def evaluate_guess(author, best_guess, inform_message, verbose=False):
    """ 
    A function that lets the user manually evaluate the best-guess employee for a given author. 
    Arguments:
        author: an author object
        best_guess: a guess object
        inform_message: an informational message will be printed to the terminal if verbose==True OR if some action is needed.
        verbose: boolean, passed from command line.
    Returns:
        A guess object if the/a guess was approved, otherwise returns False.
    """
    if isinstance(best_guess, MissingPerson):
        if verbose:
            print(f"{author.name} could not be found in the HHMI People API. No action to take.\n")
        return False
    
    if isinstance(best_guess, MultipleHits):
        print(inform_message)
        print("Multiple high scoring matches found:")
        for guess in best_guess.winners:
            print(colored(f"{guess.name}, {guess.job_title}, {guess.email}", 'blue'))
        quest = [inquirer.Checkbox('decision', 
                                   carousel=True, 
                                   message="Choose a person from the list", 
                                   choices=[guess.name for guess in best_guess.winners] + ['None of the above'], 
                                   default=['None of the above'])]
        ans = inquirer.prompt(quest, theme=BlueComposure())
        if ans['decision'] != ['None of the above']:
            return next(g for g in best_guess.winners if g.name == ans['decision'][0])
        elif ans['decision'] == ['None of the above']:
            print(f"No action will be taken for {author.name}.\n")
            return False
    else:
        if float(best_guess.score) < 85.0:
            if verbose:
                print(inform_message)
                print(
                    f"Employee best guess: {best_guess.name}, ID: {best_guess.id}, job title: {best_guess.job_title}, email: {best_guess.email}, Confidence: {round(best_guess.score, ndigits = 3)}\n"
                    )
            # Do nothing
        else:
            print(inform_message)
            print(colored(
                f"Employee best guess: {best_guess.name}, ID: {best_guess.id}, job title: {best_guess.job_title}, email: {best_guess.email}, Confidence: {round(best_guess.score, ndigits = 3)}",
                "blue"
                ))
            quest = [inquirer.List('decision', 
                                   message=f"Select {best_guess.name}?", 
                                   choices=['Yes', 'No'])]
            ans = inquirer.prompt(quest, theme=BlueComposure())
            if ans['decision'] == 'Yes':
                return(best_guess)
            else:
                print(f"No action will be taken for {author.name}.\n")
                return(False)

def confirm_action(success_message):
    """
    Ask the user to confirm whether they wish to write to the database.
    Arguments:
        success_message: a string.Template object, a message describing the change to be made
    Returns:
        True if the user confirms the change, False otherwise
    """
    quest = [inquirer.List('confirm',
                message = success_message.substitute(name=best_guess.name),
                choices = ['Yes', 'No'])]
    ans = inquirer.prompt(quest, theme=BlueComposure())
    if ans['confirm'] == 'Yes':
        return True
    else:
        print(f"No change will be made for {author.name}.\n")
        return False




### Miscellaneous low-level functions and variables

def is_janelian(author, orcid_collection):
    result = False
    if author.orcid:
        if doi_common.single_orcid_lookup(author.orcid, orcid_collection, 'orcid'):
            result = True
    if bool(re.search(r'\bJanelia\b', " ".join(author.affiliations))):
        result = True
    return(result)

def choose_authors_manually(author_list, pre_selected_authors=None):
    if pre_selected_authors is None:
        pre_selected_authors = []
    else:
        pre_selected_authors = [a.name for a in pre_selected_authors]
    print("Crossref has no author affiliations for this paper.")
    author_names = [a.name for a in author_list]
    default_selections = [n for n in author_names if n in pre_selected_authors]
    quest = [inquirer.Checkbox('decision', carousel=True, message="Choose Janelia authors", choices=author_names, default=default_selections)]
    ans1 = inquirer.prompt(quest, theme=BlueComposure())
    quest = [inquirer.List('confirm', message = f"Confirm Janelia authors:\n{ans1['decision']}", choices=['Yes', 'No']) ]
    ans2 = inquirer.prompt(quest, theme=BlueComposure())
    if ans2['confirm'] == 'Yes':
        return ans1['decision']
    else:
        print('Exiting program.')
        sys.exit(0)

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

def get_doi_record(doi):
    result = JRC.call_crossref(doi)
    return( result['message'] )

def strip_orcid_if_provided_as_url(orcid):
    prefixes = ["http://orcid.org/", "https://orcid.org/"]
    for prefix in prefixes:
        if orcid.startswith(prefix):
            return orcid[len(prefix):]
    return(orcid)



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
        if not isinstance(msg, str):
            msg = f"An exception of type {type(msg).__name__} occurred. Arguments:\n{msg.args}"
            LOGGER.critical(msg)
            sys.exit(-1 if msg else 0)

# Old code that should really be added to our documentation instead of lingering here
# api_key = os.environ.get('PEOPLE_API_KEY')
# if not api_key:
#     print("Error: Please set the environment variable PEOPLE_API_KEY.")
#     sys.exit(1)





# -----------------------------------------------------------------------------

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
    description = "Given a DOI, use fuzzy name matching to correlate Janelia authors who don't have ORCIDs to Janelia employees. Update ORCID records as needed.")
    parser.add_argument('--doi', dest='doi', action='store', required=True,
                         help='DOI whose authors will be processed.')
    parser.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    parser.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    arg = parser.parse_args()
    LOGGER = JRC.setup_logging(arg)

    initialize_program()

    orcid_collection = DB['dis'].orcid
    doi_collection = DB['dis'].dois
    #doi='10.1101/2021.08.18.456004'
    #doi='10.7554/eLife.80660'
    #doi='10.1101/2024.05.09.593460' #BUG! The menu to select authors manually doesn't show all authors
    #doi='10.1021/jacs.4c03092'
    #doi='10.1101/2023.12.19.572369'
    doi_record = get_doi_record(arg.doi)
    all_authors = create_author_objects(doi_record)
    janelian_authors = [ a for a in all_authors if is_janelian(a, orcid_collection) ]
    if not any([a.affiliations for a in all_authors]):
        names_picked = choose_authors_manually(all_authors, pre_selected_authors=janelian_authors)
        janelian_authors = [ a for a in all_authors if a.name in names_picked ]
    
    for author in janelian_authors:

        if author.orcid:
            mongo_orcid_record = doi_common.single_orcid_lookup(author.orcid, orcid_collection, 'orcid')
            if mongo_orcid_record:
                if 'employeeId' in mongo_orcid_record:
                    if arg.VERBOSE:
                        print( f"{author.name} is in our ORCID collection, with both an ORCID an employee ID. No action to take.\n" )
                    # Do nothing
                elif 'employeeId' not in mongo_orcid_record:
                    inform_message = f"{author.name} is in our ORCID collection, but without an employee ID."
                    best_guess = guess_employee(author)
                    proceed = evaluate_guess(author, best_guess, inform_message, verbose=arg.VERBOSE) 
                    if proceed:
                        best_guess = proceed # if there were multiple best guesses, assign the user-selected one
                        success_message = string.Template("Confirm you wish to add $name's employee ID to their existing ORCID record") #can't use best_guess.name bc guess may be None (MissingPerson)
                        confirm_proceed = confirm_action(success_message)
                        if confirm_proceed:
                            doi_common.update_existing_orcid(lookup=author.orcid, add=best_guess.id, coll=orcid_collection, lookup_by='orcid')
                            
            elif not mongo_orcid_record:
                inform_message = f"{author.name} has an ORCID on this paper, but this ORCID is not in our collection."
                best_guess = guess_employee(author)
                proceed = evaluate_guess(author, best_guess, inform_message, verbose=arg.VERBOSE)
                if proceed:
                    best_guess = proceed # if there were multiple best guesses, assign the user-selected one
                    employeeId_result = doi_common.single_orcid_lookup(best_guess.id, orcid_collection, lookup_by='employeeId')
                    success_message = ''
                    if employeeId_result:
                        if 'orcid' not in employeeId_result:
                            print(f"{author.name} is in our collection, with an employee ID only.")
                            success_message = string.Template("Confirm you wish to add an ORCID id to the existing record for $name")
                            confirm_proceed = confirm_action(success_message)
                            if confirm_proceed:
                                doi_common.update_existing_orcid(lookup=best_guess.id, add=author.orcid, coll=orcid_collection, lookup_by='employeeId')
                        if 'orcid' in employeeId_result: # Hopefully this will never get triggered
                            print(f"{author.name}'s ORCID is {author.orcid} on the paper, but it's {employeeId_result['orcid']} in our collection. Aborting program.")
                            sys.exit(1)
                    else:
                        print(f"{author.name} has an ORCID on this paper, and they are not in our collection.")
                        success_message = string.Template("Confirm you wish to create an ORCID record for $name, with both their employee ID and their ORCID")
                        confirm_proceed = confirm_action(success_message)
                        if confirm_proceed:
                            doi_common.add_orcid(best_guess.id, orcid_collection, given=best_guess.generate_family_name_permutations(), family=best_guess.last_names, orcid=author.orcid)
        
        elif not author.orcid:
            inform_message = f"{author.name} does not have an ORCID on this paper."
            success_message = ''
            best_guess = guess_employee(author)
            if isinstance(best_guess, MissingPerson):
                if arg.VERBOSE == True:
                    print(f"{author.name} could not be found in the HHMI People API. No action to take.\n")
            elif isinstance(best_guess, MultipleHits):
                    proceed = evaluate_guess(author, best_guess, inform_message, verbose=arg.VERBOSE)
                    best_guess = proceed # if there were multiple best guesses, assign the user-selected one
                    if proceed:
                        employeeId_result = doi_common.single_orcid_lookup(best_guess.id, orcid_collection, lookup_by='employeeId')
                        if employeeId_result:
                            if 'orcid' in employeeId_result:
                                print(f"{author.name} is in our collection with both an ORCID and an employee ID. No action to take.\n")
                                # Do nothing
                            else:
                                print(f"{author.name} is in our collection with an employee ID only. No action to take.\n")
                                # Do nothing
                        else:
                            print(f"There is no record in our collection for {author.name}.")
                            success_message = string.Template("Confirm you wish to create an ORCID record for $name with an employee ID only")
                            confirm_proceed = confirm_action(success_message)
                            if confirm_proceed:
                                doi_common.add_orcid(best_guess.id, orcid_collection, given=best_guess.generate_family_name_permutations(), family=best_guess.last_names, orcid=None)
            elif isinstance(best_guess, Employee):
                employeeId_result = doi_common.single_orcid_lookup(best_guess.id, orcid_collection, lookup_by='employeeId')
                if employeeId_result:
                    if 'orcid' in employeeId_result:
                        if arg.VERBOSE == True:
                            print(f"{author.name} is in our collection with both an ORCID and an employee ID. No action to take.\n")
                            # Do nothing
                    else:
                        if arg.VERBOSE == True:
                            print(f"{author.name} is in our collection with an employee ID only. No action to take.\n")
                            # Do nothing
                else:
                    proceed = evaluate_guess(author, best_guess, inform_message, verbose=arg.VERBOSE)
                    if proceed:
                        print(f"There is no record in our collection for {author.name}.")
                        success_message = string.Template("Confirm you wish to create an ORCID record for $name with an employee ID only")
                        confirm_proceed = confirm_action(success_message)
                        if confirm_proceed:
                            doi_common.add_orcid(best_guess.id, orcid_collection, given=best_guess.generate_family_name_permutations(), family=best_guess.last_names, orcid=None)
                                                                




                              



#For bug testing, do not run!!!
# import name_match as nm
# nm.initialize_program()
# orcid_collection = nm.DB['dis'].orcid
# doi_collection = nm.DB['dis'].dois
# doi='10.1101/2023.12.19.572369'
# doi_record = nm.get_doi_record(doi)
# all_authors = nm.create_author_objects(doi_record)
# janelian_authors = [ a for a in all_authors if nm.is_janelian(a, orcid_collection) ]
# if not any([a.affiliations for a in all_authors]):
#     names_picked = nm.choose_authors_manually(all_authors, pre_selected_authors=janelian_authors)
#     janelian_authors = [ a for a in all_authors if a.name in names_picked ]

# janelian_authors[14]
