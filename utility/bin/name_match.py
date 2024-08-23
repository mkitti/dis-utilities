"""
Given a DOI, try to identify Janelia authors who don't have ORCIDs and correlate them with employees.
Update ORCID records as needed.
"""

import sys
import argparse
import re
import itertools
from collections import Counter
from collections.abc import Iterable
from operator import attrgetter
from nameparser import HumanName
from rapidfuzz import fuzz, utils
from unidecode import unidecode
from termcolor import colored
import inquirer
from inquirer.themes import BlueComposure
import jrc_common.jrc_common as JRC
import doi_common.doi_common as doi_common

#TODO: Add some of these imports to requirements.txt?
#TODO: Add new names to an existing record?
#TODO: Add support for arxiv DOIs
#TODO: Add a little more info to the yellow prompt beyond just job title and supOrg. Maybe use managerId?
#TODO: If nothing can be done for an employee, don't prompt the user to confirm them.

# authors_to_check: a list. If the paper has affiliations, the list is just those with janelia affiliations. Otherwise, the list is all authors.
# revised_jrc_authors = []
# for author in authors_to_check:
#   if author has an orcid on the paper:
#       if that orcid has a record in our collection:
#           if that record has an employeeId:
#               Add any new names to the existing record
#               Add employeeId to revised_jrc_authors
#           elif that record does not have an employeeId:
#               Search the people API. 
#               If user confirms the match:
#                   Add employeeId and any new names to the existing record
#                   Add employeeId to revised_jrc_authors
#       elif that orcid does not have a record in our collection:
#           Search the People API.
#           If user confirms the match:
#               Create a record with both orcid and employeeId
#               Add employeeId to revised_jrc_authors
#   elif author does not have an orcid on the paper:
#       Search the People API.
#       If user confirms the match:
#           If a record with that employeeId already exists in our collection:
#               Add any new names to the existing record
#               Add employeeId to revised_jrc_authors
#           else:
#               Create a record for that employee with their employeeId and their names.
#               Add employeeId to revised_jrc_authors
#               
# add any employeeIds in revised_jrc_authors to jrc authors if they are not in jrc_authors already





class Author:
    """ Author objects are constructed solely from the Crossref-provided author information. """
    def __init__(self, name, orcid=None, affiliations=None):
        self.name = name
        self.orcid = orcid
        self.affiliations = affiliations if affiliations is not None else [] # Need to avoid the python mutable arguments trap

class Employee:
    """ Employees are constructed from information found in the HHMI People database. """
    def __init__(self, id=None, job_title=None, email=None, location=None, supOrgName=None, first_names=None, middle_names=None, last_names=None, exists=False):
        self.id = id
        self.job_title = job_title
        self.email = email
        self.location = location
        self.supOrgName = supOrgName
        self.first_names = list(set(first_names)) if first_names is not None else [] # Need to avoid the python mutable arguments trap
        self.middle_names = list(set(middle_names)) if middle_names is not None else []
        self.last_names = list(set(last_names)) if last_names is not None else []
        self.exists = exists
    def exists(self):
        return(self.exists)

class Guess(Employee):
    """ A Guess is a subtype of Employee that consists of just ONE name permutation 
    (e.g. Gerald M Rubin) and a fuzzy match score (calculated before the guess object is instantiated). """
    def __init__(self, id=None, job_title=None, email=None, location=None, supOrgName=None, first_names=None, middle_names=None, last_names=None, exists=False, name=None, score=None):
        super().__init__(id, job_title, email, location, supOrgName, first_names, middle_names, last_names, exists)
        self.name = name
        self.score = score

class MongoOrcidRecord:
    def __init__(self, orcid=None, employeeId=None, exists=False):
        self.orcid = orcid
        self.employeeId = employeeId
        self.exists = exists
    def has_orcid(self):
        return(True if self.orcid else False)
    def has_employeeId(self):
        return(True if self.employeeId else False)


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
    idsearch_results = search_people_api(id, mode='id')
    if idsearch_results:
        job_title = job_title = idsearch_results['businessTitle'] if 'businessTitle' in idsearch_results else None
        email = idsearch_results['email'] if 'email' in idsearch_results else None
        location = idsearch_results['locationName'] if 'locationName'in idsearch_results else None # will be 'Janelia Research Campus' for janelians
        supOrgName = idsearch_results['supOrgName'] if 'supOrgName' in idsearch_results and any(idsearch_results['supOrgName']) else None
        first_names = [ idsearch_results['nameFirstPreferred'], idsearch_results['nameFirst'] ]
        middle_names = [ idsearch_results['nameMiddlePreferred'], idsearch_results['nameMiddle'] ]
        last_names = [ idsearch_results['nameLastPreferred'], idsearch_results['nameLast'] ]
        return(
            Employee(
            id=id,
            job_title=job_title,
            email=email,
            location=location,
            supOrgName = supOrgName,
            first_names=first_names, 
            middle_names=middle_names,
            last_names=last_names,
            exists=True)
        )
    else:
        return(Employee(exists=False))
        

def create_guess(employee, name=None, score=None):
    return(Guess(
        employee.id, 
        employee.job_title, 
        employee.email,
        employee.location,
        employee.supOrgName, 
        employee.first_names, 
        employee.middle_names, 
        employee.last_names,
        employee.exists, 
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
        A list of guess objects. This list will never be empty. It may, however, simply contain one 'empty' guess object.
    """
    name = HumanName(author.name)
    basic = name_search(name.first, name.last)
    stripped = name_search(unidecode(name.first), unidecode(name.last)) # decode accents and other special characters
    hyphen_split1 = name_search(name.first, name.last.split('-')[0]) if '-' in name.last else None # try different parts of a hyphenated last name
    hyphen_split2 = name_search(name.first, name.last.split('-')[1]) if '-' in name.last else None
    strp_hyph1 = name_search(unidecode(name.first), unidecode(name.last.split('-')[0])) if '-' in name.last else None # split on hyphen and decoded
    strp_hyph2 = name_search(unidecode(name.first), unidecode(name.last.split('-')[1])) if '-' in name.last else None
    two_middle_names1 = name_search(name.first, name.middle.split(' ')[0]) if len(name.middle.split())==2 else None # try different parts of a multi-word middle name, e.g. Virginia Marjorie Tartaglio Scarlett
    two_middle_names2 = name_search(name.first, name.middle.split(' ')[1]) if len(name.middle.split())==2 else None
    strp_middle1 = name_search(unidecode(name.first), unidecode(name.middle.split()[0])) if len(name.middle.split())==2 else None # split on middle name space and decoded
    strp_middle2 = name_search(unidecode(name.first), unidecode(name.middle.split()[1])) if len(name.middle.split())==2 else None

    all_results = [basic, stripped, hyphen_split1, hyphen_split2, strp_hyph1, strp_hyph2, two_middle_names1, two_middle_names2, strp_middle1, strp_middle2]
    candidate_ids = [id for id in list(set(flatten(all_results))) if id is not None]
    candidate_employees = [create_employee(id) for id in candidate_ids]
    candidate_employees = [e for e in candidate_employees if e.location == 'Janelia Research Campus']
    return(fuzzy_match(author, candidate_employees))


def name_search(first, last):
    """ 
    Arguments: 
        first: first name, a string.
        last: last name, a string.
    Returns:
        A list of candidate employee ids (strings) OR None.
    """
    search_results1 = search_people_api(first, mode='name') # a list of dicts
    search_results2 = search_people_api(last, mode='name')
    if search_results1 and search_results2:
        return( process_search_results(search_results1, search_results2) )
    else:
        return(None)

def process_search_results(list1, list2):
    """
    A function to enforce that the same employeeId must appear in both the first and last name searches to return a successful result.
    Arguments:
        list1: a list of dicts, where each dict is the metadata for an employee from our People search on the first name. (e.g., [a dict for Virginia Scarlett, a dict for Virginia Ruetten])
        list2: a list of dicts, where each dict is the metadata for an employee from our People search on the last name. (e.g., [a dict for Virginia Scarlett, a dict for Scarlett Pitts])
    Returns:
        A list of employee Ids from dicts that occurred in both lists (e.g., [Virginia Scarlett's employeeId])
        OR
        None
    """
    employee_ids_list1 = {item['employeeId'] for item in list1}
    employee_ids_list2 = {item['employeeId'] for item in list2}
    common_ids = list(employee_ids_list1.intersection(employee_ids_list2))
    if common_ids:
        return(common_ids)
    else:
        return(None)

def fuzzy_match(author, candidate_employees):
    """ 
    Arguments: 
        author: an author object.
        candidate_employees: a list of employee objects, possibly an empty list.
    Returns:
        A list of guess objects. This list will never be empty. It may, however, simply contain one 'empty' guess object.
    """
    guesses = []
    if candidate_employees:
        for employee in candidate_employees:
            employee_permuted_names = generate_name_permutations(employee.first_names, employee.middle_names, employee.last_names)
            for name in employee_permuted_names:
                guesses.append(create_guess(employee, name=name)) # Each employee will generate several guesses, e.g. Virginia T Scarlett, Virginia Scarlett, Ginnie Scarlett
    if guesses:
        for guess in guesses:
            guess.score = fuzz.token_sort_ratio(author.name, guess.name, processor=utils.default_process) #processor will convert the strings to lowercase, remove non-alphanumeric characters, and trim whitespace
        high_score = max( [g.score for g in guesses] )
        winners = [ g for g in guesses if g.score == high_score ]
        return(winners)
    elif not guesses:
        return( [ Guess(exists=False) ] )


def evaluate_guess(author, candidates, inform_message, verbose=False):
    """ 
    A function that lets the user manually evaluate the best-guess employee for a given author. 
    Arguments:
        author: an author object.
        candidates: a non-empty list of guess objects. 
        inform_message: an informational message will be printed to the terminal if verbose==True OR if some action is needed.
            one of:
                f"{author.name} is in our ORCID collection, but without an employee ID."
                f"{author.name} has an ORCID on this paper, but this ORCID is not in our collection."
                f"{author.name} does not have an ORCID on this paper."
        verbose: a boolean, passed from command line.
    Returns:
        A guess object. If this guess.exists == False, this indicates that the guess was rejected, either by the user or automatically due to low score.
    """    
    if len(candidates) > 1:
        print(inform_message)
        print(f"Multiple high scoring matches found for {author.name}:")
        # Some people appear twice in the HHMI People system. 
        # inquirer gives us know way of knowing whether the user selected the first instance of 'David Clapham' or the second instance of 'David Clapham'.
        # We are adding some code to append numbers to the names to make them unique, and then use the index of the selected object to grab the original object.
        repeat_names = [name for name, count in Counter(guess.name for guess in candidates).items() if count > 1]
        selection_list = []
        counter = {}
        for guess in candidates:
            if guess.name in repeat_names:
                if guess.name in counter:
                    selection_list.append(Guess(id=guess.id,job_title=guess.job_title,email=guess.email,location=guess.location,supOrgName = guess.supOrgName,first_names=guess.first_names,middle_names=guess.middle_names,last_names=guess.last_names, exists=guess.exists,
                                                name=guess.name+f'-{counter[guess.name]+1}',score=guess.score))
                    counter[guess.name] += 1
                else:
                    selection_list.append(Guess(id=guess.id,job_title=guess.job_title,email=guess.email,location=guess.location,supOrgName = guess.supOrgName,first_names=guess.first_names,middle_names=guess.middle_names,last_names=guess.last_names, exists=guess.exists,
                                                name=guess.name+f'-1',score=guess.score))
                    counter[guess.name] = 1
            else:
                selection_list.append(guess)
        for guess in selection_list:
            print(colored(f"{guess.name}, ID: {guess.id}, job title: {guess.job_title}, supOrgName: {guess.supOrgName}, email: {guess.email}", 'black', 'on_yellow'))
        quest = [inquirer.Checkbox('decision', 
                                   carousel=True, 
                                   message="Choose a person from the list", 
                                   choices=[guess.name for guess in selection_list] + ['None of the above'], 
                                   default=['None of the above'])]
        ans = inquirer.prompt(quest, theme=BlueComposure()) # returns {'decision': a list}, e.g. {'decision': ['Virginia Scarlett']}
        while len(ans['decision']) > 1: 
            print('Please choose only one option.')
            quest = [inquirer.Checkbox('decision', 
                            carousel=True, 
                            message="Choose a person from the list", 
                            choices=[guess.name for guess in selection_list] + ['None of the above'], 
                            default=['None of the above'])]
            ans = inquirer.prompt(quest, theme=BlueComposure()) 
        if ans['decision'] != ['None of the above']:
            i = [guess.name for guess in selection_list].index(ans['decision'][0])
            return(candidates[i])
            #return next(g for g in candidates if g.name == ans['decision'][0]) 
        elif ans['decision'] == ['None of the above']:
            print(f"No action will be taken for {author.name}.\n")
            return( Guess(exists=False) )

    elif len(candidates) == 1:
        best_guess = candidates[0]
        if not best_guess.exists:
            if verbose:
                print(f"A Janelian named {author.name} could not be found in the HHMI People API. No action to take.\n")
            return(best_guess)
        if float(best_guess.score) < 85.0:
            if verbose:
                print(inform_message)
                print(
                    f"Employee best guess: {best_guess.name}, ID: {best_guess.id}, job title: {best_guess.job_title}, supOrgName: {best_guess.supOrgName}, email: {best_guess.email}, Confidence: {round(best_guess.score, ndigits = 3)}\n"
                    )
            return( Guess(exists=False) )
        elif float(best_guess.score) > 85.0:
            print(inform_message)
            print(colored(
                f"Employee best guess: {best_guess.name}, ID: {best_guess.id}, job title: {best_guess.job_title}, supOrgName: {best_guess.supOrgName}, email: {best_guess.email}, Confidence: {round(best_guess.score, ndigits = 3)}",
                "black", "on_yellow"
                ))
            quest = [inquirer.List('decision', 
                                   message=f"Select {best_guess.name}?", 
                                   choices=['Yes', 'No'])]
            ans = inquirer.prompt(quest, theme=BlueComposure())
            if ans['decision'] == 'Yes':
                return(best_guess)
            else:
                print(f"No action will be taken for {author.name}.\n")
                return( Guess(exists=False) )

def confirm_action(success_message):
    """
    Ask the user to confirm whether they wish to write to the database.
    Arguments:
        success_message: a string, a message describing the change to be made
    Returns:
        True if the user confirms the change, False otherwise
    """
    quest = [inquirer.List('confirm',
                message = success_message,
                choices = ['Yes', 'No'])]
    ans = inquirer.prompt(quest, theme=BlueComposure())
    if ans['confirm'] == 'Yes':
        return True
    else:
        print(f"No change will be made for {author.name}.\n")
        return False


### Miscellaneous low-level functions and variables

def determine_authors_to_check(doi_record):
    all_authors = create_author_objects(doi_record)
    if not any([a.affiliations for a in all_authors]):
        return(all_authors)
    else:
        return([ a for a in all_authors if is_janelian(a, orcid_collection) ])

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

def create_orcid_record(best_guess, orcid_collection, author):
    doi_common.add_orcid(best_guess.id, orcid_collection, given=first_names_for_orcid_record(author, best_guess), family=last_names_for_orcid_record(author, best_guess), orcid=author.orcid)
    #doi_common.update_jrc_author(doi, doi_collection, orcid_collection)
    print(f"Record created for {author.name} in orcid collection.")

def generate_name_permutations(first_names, middle_names, last_names):
        permutations = set()
        # All possible first names + all possible last names
        for first_name, last_name in itertools.product(first_names, last_names):
            permutations.add(
                f"{first_name} {last_name}"
            )
        # All possible first names + all possible middle names + all possible last names
        if middle_names:
            if any(item for item in middle_names if item): # check for ['', '']
                for first_name, middle_name, last_name in itertools.product(first_names, middle_names, last_names):
                    permutations.add(
                        f"{first_name} {middle_name} {last_name}"
                    )
            # All possible first names + all possible middle initials + all possible last names
                for first_name, middle_name, last_name in itertools.product(first_names, middle_names, last_names):
                    middle_initial = middle_name[0]
                    permutations.add(
                        f"{first_name} {middle_initial} {last_name}"
                    )
        return list(sorted(permutations))

def first_names_for_orcid_record(author, employee):
    result = generate_name_permutations(
        [HumanName(author.name).first]+employee.first_names, 
        [HumanName(author.name).middle]+employee.middle_names,
        [HumanName(author.name).last]+employee.last_names
    )
    h_result = [HumanName(n) for n in result]
    return(list(set([' '.join((n.first,n.middle)).strip() for n in h_result])))

def last_names_for_orcid_record(author, employee):
    result = generate_name_permutations(
        [HumanName(author.name).first]+employee.first_names, 
        [HumanName(author.name).middle]+employee.middle_names,
        [HumanName(author.name).last]+employee.last_names
    )
    h_result = [HumanName(n) for n in result]
    return(list(set([n.last for n in h_result])))

def search_people_api(query, mode):
    response = None
    if mode not in {'name', 'id'}:
        raise ValueError("HHMI People API search mode must be either 'name' or 'id'.")
    if mode == 'name':
        response = JRC.call_people_by_name(query)
    elif mode == 'id':
        response = JRC.call_people_by_id(query)
    return(response)

def flatten(xs): # https://stackoverflow.com/questions/2158395/flatten-an-irregular-arbitrarily-nested-list-of-lists
    for x in xs:
        if isinstance(x, Iterable) and not isinstance(x, (str, bytes)):
            yield from flatten(x)
        else:
            yield x

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



# WORK IN PROGRESS:

# def update_orcid_record_names_if_needed(author, employee, mongo_orcid_record):
#     if any(first_names_for_orcid_record(author, employee) not in mongo_orcid_record['given']):
#         #ADD NEW NAMES! TODO for Rob




def get_mongo_orcid_record(search_term):
    if not search_term:
        return(MongoOrcidRecord(exists=False))
    else:
        result = ''
        if len(search_term) == 19:
            result = doi_common.single_orcid_lookup(search_term, orcid_collection, 'orcid')
        else:
            result = doi_common.single_orcid_lookup(search_term, orcid_collection, 'employeeId')
        if result:
            if 'orcid' in result and 'employeeId' in result:
                return(MongoOrcidRecord(orcid=result['orcid'], employeeId=result['employeeId'], exists=True))
            if 'orcid' in result and 'employeeId' not in result:
                return(MongoOrcidRecord(orcid=result['orcid'], exists=True))
            if 'orcid' not in result and 'employeeId' in result:
                return(MongoOrcidRecord(employeeId=result['employeeId'], exists=True))
        else:
            return(MongoOrcidRecord(exists=False))






# -----------------------------------------------------------------------------

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
    description = "Given a DOI, use fuzzy name matching to correlate Janelia authors who don't have ORCIDs to Janelia employees. Update ORCID records as needed.")
    muexgroup = parser.add_mutually_exclusive_group(required=True)
    muexgroup.add_argument('--doi', dest='DOI', action='store',
                         help='Produce a citation from a single DOI.')
    muexgroup.add_argument('--file', dest='FILE', action='store',
                         help='Produce a citation from a file containing one or more DOIs.')
    parser.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    parser.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    arg = parser.parse_args()
    LOGGER = JRC.setup_logging(arg)

    initialize_program()
    orcid_collection = DB['dis'].orcid
    doi_collection = DB['dis'].dois

    dois = [arg.DOI.lower()] if arg.DOI else [] # .lower() because our collection is case-sensitive
    if arg.FILE:
        try:
            with open(arg.FILE, "r", encoding="ascii") as instream:
                for doi in instream.read().splitlines():
                    dois.append(doi.lower())
        except Exception as err:
            print(f"Could not process {arg.FILE}")
            exit()

    for doi in dois:

        doi_record = get_doi_record(doi)
        print(f"{doi}: {doi_record['title'][0]}")
        authors_to_check = determine_authors_to_check(doi_record) # A list. If the paper has affiliations, the list is just those with janelia affiliations. Otherwise, all authors.
        #revised_jrc_authors = []

        for author in authors_to_check:

            if author.orcid:
                mongo_orcid_record = get_mongo_orcid_record(author.orcid)
                if mongo_orcid_record.exists:
                    if mongo_orcid_record.has_employeeId():
                        employee = create_employee(mongo_orcid_record.employeeId)
                        #TODO: update_orcid_record_names_if_needed(author, employee, mongo_orcid_record)
                        #revised_jrc_authors.append(employee.id)
                        if arg.VERBOSE:
                            print( f"{author.name} is in our ORCID collection, with both an ORCID an employee ID. No action to take.\n" )
                    else:
                        inform_message = f"{author.name} is in our ORCID collection, but without an employee ID."
                        candidates = guess_employee(author)
                        approved_guess = evaluate_guess(author, candidates, inform_message, verbose=arg.VERBOSE) 
                        if approved_guess.exists:
                            success_message = f"Confirm you wish to add {approved_guess.name}'s employee ID to their existing ORCID record"
                            proceed = confirm_action(success_message)
                            if proceed:
                                doi_common.update_existing_orcid(lookup=author.orcid, add=approved_guess.id, coll=orcid_collection, lookup_by='orcid')
                                #TODO: ^ Add names to this
                                #revised_jrc_authors.append(approved_guess.id)

                elif not mongo_orcid_record.exists:
                    inform_message = f"{author.name} has an ORCID on this paper, but this ORCID is not in our collection."
                    candidates = guess_employee(author)
                    records = [get_mongo_orcid_record(guess.employeeId) for guess in candidates if guess.exists]
                    # if nothing can be done for all candidates, then proceed no further.
                    if records:
                        if all( [mongo_record.has_orcid() and mongo_record.has_employeeId() for mongo_record in records] ):
                            pass
                        else:
                            approved_guess = evaluate_guess(author, candidates, inform_message, verbose=arg.VERBOSE)
                            if approved_guess.exists:
                                mongo_orcid_record = get_mongo_orcid_record(approved_guess.id) # I know, I'm doing another lookup, which is slow.
                                success_message = ''
                                if mongo_orcid_record:
                                    if not mongo_orcid_record.has_orcid():
                                        print(f"{author.name} is in our collection, with an employee ID only.")
                                        success_message = f"Confirm you wish to add an ORCID id to the existing record for {approved_guess.name}"
                                        proceed = confirm_action(success_message)
                                        if proceed:
                                            doi_common.update_existing_orcid(lookup=approved_guess.id, add=author.orcid, coll=orcid_collection, lookup_by='employeeId')
                                            #TODO: ^ Add names to this
                                            #revised_jrc_authors.append(approved_guess.id)
                                    elif mongo_orcid_record.has_orcid(): # Hopefully this will never get triggered, i.e., if one person has two ORCIDs
                                        print(f"{author.name}'s ORCID is {author.orcid} on the paper, but it's {employeeId_result['orcid']} in our collection. Aborting program.")
                                        sys.exit(1)
                                else:
                                    print(f"{author.name} has an ORCID on this paper, and they are not in our collection.")
                                    success_message = f"Confirm you wish to create an ORCID record for {approved_guess.name}, with both their employee ID and their ORCID"
                                    proceed = confirm_action(success_message)
                                    if proceed:
                                        create_orcid_record(approved_guess, orcid_collection, author)
                                        #revised_jrc_authors.append(approved_guess.id)


            elif not author.orcid:
                inform_message = f"{author.name} does not have an ORCID on this paper."
                candidates = guess_employee(author)
                records = [get_mongo_orcid_record(guess.id) for guess in candidates if guess.exists]
                if records: 
                    if all( [mongo_record.has_employeeId() for mongo_record in records] ): # if nothing can be done for all candidates, then proceed no further.
                        print(f'All matches to {author.name} are in our collection with employeeIds. No action to take.\n')
                        #print(f"{author.name} is in our collection with an employee ID only. No action to take.\n")
                            #TODO: update_orcid_record_names_if_needed(author, employee, mongo_orcid_record)
                            #revised_jrc_authors.append(approved_guess.id)
                    else:
                        approved_guess = evaluate_guess(author, candidates, inform_message, verbose=arg.VERBOSE)
                        mongo_orcid_record = get_mongo_orcid_record(approved_guess.id)
                        if mongo_orcid_record.has_employeeId():
                            print(f"{author.name} is in our collection with an employee ID. No action to take.\n")
                            #TODO: update_orcid_record_names_if_needed(author, employee, mongo_orcid_record)
                            #revised_jrc_authors.append(approved_guess.id)
                        else:
                            if approved_guess.exists:
                                print(f"There is no record in our collection for {author.name}.")
                                success_message = f"Confirm you wish to create an ORCID record for {approved_guess.name} with an employee ID only"
                                proceed = confirm_action(success_message)
                                if proceed:
                                    create_orcid_record(approved_guess, orcid_collection, author)
                                    #revised_jrc_authors.append(approved_guess.id)

        #jrc_authors = doi_record['jrc_author']
        #print( list(set(revised_jrc_authors).union(set(jrc_authors))) )
        doi_common.update_jrc_author(doi, doi_collection, orcid_collection)



#For bug testing, do not run!!!
# import name_match as nm
# nm.initialize_program()
# orcid_collection = nm.DB['dis'].orcid
# doi_collection = nm.DB['dis'].dois
# doi='10.1101/2024.06.30.601394'
# doi_record = nm.get_doi_record(doi)

    #doi='10.1038/s41587-022-01524-7'
    #doi='10.7554/elife.80660'
    #doi='10.1101/2024.05.09.593460'
    #doi='10.1021/jacs.4c03092'
    #doi='10.1038/s41556-023-01154-4'
    #doi='10.7554/eLife.80622'




            # if author.orcid:
            #     #mongo_orcid_record = doi_common.single_orcid_lookup(author.orcid, orcid_collection, 'orcid')
            #     mongo_orcid_record = get_mongo_orcid_record(author.orcid, 'orcid')
            #     #if mongo_orcid_record:
            #     if mongo_orcid_record.exists:
            #         if 'employeeId' in mongo_orcid_record:
            #             employee = create_employee(mongo_orcid_record['employeeId'])
            #             #TODO: update_orcid_record_names_if_needed(author, employee, mongo_orcid_record)
            #             #revised_jrc_authors.append(employee.id)
            #             if arg.VERBOSE:
            #                 print( f"{author.name} is in our ORCID collection, with both an ORCID an employee ID. No action to take.\n" )
            #         elif 'employeeId' not in mongo_orcid_record:
            #             inform_message = f"{author.name} is in our ORCID collection, but without an employee ID."
            #             candidates = guess_employee(author)
            #             approved_guess = evaluate_guess(author, candidates, inform_message, verbose=arg.VERBOSE) 
            #             if approved_guess.exists:
            #                 success_message = f"Confirm you wish to add {approved_guess.name}'s employee ID to their existing ORCID record"
            #                 proceed = confirm_action(success_message)
            #                 if proceed:
            #                     doi_common.update_existing_orcid(lookup=author.orcid, add=approved_guess.id, coll=orcid_collection, lookup_by='orcid')
            #                     #TODO: ^ Add names to this
            #                     #revised_jrc_authors.append(approved_guess.id)
