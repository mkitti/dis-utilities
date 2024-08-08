"""
Given a DOI, try to identify Janelia authors who don't have ORCIDs and correlate them with employees.
Update ORCID records as needed.
"""

import sys
import string
import argparse
import re
import itertools
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
#TODO: When you get multiple hits, do a fuzzy name match on the full name. Eliminate candidates below a 90% threshold.

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
#   
#                   
#
#               
#       

class Author:
    """ Author objects are constructed solely from the Crossref-provided author information. """
    def __init__(self, name, orcid=None, affiliations=None):
        self.name = name
        self.orcid = orcid
        self.affiliations = affiliations if affiliations is not None else [] # Need to avoid the python mutable arguments trap

class Employee:
    """ Employees are constructed from information found in the HHMI People database. """
    def __init__(self, id, job_title=None, email=None, location=None, supOrgName=None, first_names=None, middle_names=None, last_names=None):
        self.id = id
        self.job_title = job_title
        self.email = email
        self.location = location
        self.supOrgName = supOrgName
        self.first_names = list(set(first_names)) if first_names is not None else [] # Need to avoid the python mutable arguments trap
        self.middle_names = list(set(middle_names)) if middle_names is not None else []
        self.last_names = list(set(last_names)) if last_names is not None else []

class Guess(Employee):
    """ A Guess is a subtype of Employee that consists of just ONE name permutation 
    (e.g. Gerald M Rubin) and a fuzzy match score (calculated before the guess object is instantiated). """
    def __init__(self, id, job_title=None, email=None, location=None, supOrgName=None, first_names=None, middle_names=None, last_names=None, name=None, score=None):
        super().__init__(id, job_title, email, location, supOrgName, first_names, middle_names, last_names)
        self.name = name
        self.score = score

class MissingPerson:
    """ This class indicates that searching the HHMI People API yielded no results. """
    pass

class MultipleHits:
    """ 
    This class indicates that an author name matched multiple HHMI employees with equally high scores. 
    This class's only attribute is a list of employee objects. These employees' names had an equally 
    high fuzzy match score against the author name (therefore I call them 'winners').
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
    idsearch_results = search_people_api(id, mode='id')
    if not isinstance(idsearch_results, MissingPerson):
        job_title = job_title = idsearch_results['businessTitle'] if 'businessTitle' in idsearch_results else None
        email = idsearch_results['email'] if 'email' in idsearch_results else None
        location = idsearch_results['locationName'] if 'locationName'in idsearch_results else None # will be 'Janelia Research Campus' for janelians
        supOrgName = idsearch_results['supOrgName'] if 'supOrgName' in idsearch_results and any(idsearch_results['supOrgName']) else None
        first_names = [ idsearch_results['nameFirstPreferred'], idsearch_results['nameFirst'] ]
        middle_names = [ idsearch_results['nameMiddlePreferred'], idsearch_results['nameMiddle'] ]
        last_names = [ idsearch_results['nameLastPreferred'], idsearch_results['nameLast'] ]
        return(
            Employee(
            id,
            job_title=job_title,
            email=email,
            location=location,
            supOrgName = supOrgName,
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
        employee.location,
        employee.supOrgName, 
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
    candidate_employees = []
    search_term = max(author.name.split(), key=len) # We can only search the People API by one name, so just pick the longest one
    namesearch_results = search_people_api(search_term, mode='name')
    if isinstance(namesearch_results, MissingPerson): 
        namesearch_results = search_tricky_names(author.name)
    if not isinstance(namesearch_results, MissingPerson):
        candidate_employee_ids = [ employee_dic['employeeId'] for employee_dic in namesearch_results ]
        for id in candidate_employee_ids:
            employee_from_id_search = create_employee(id) 
            if not isinstance(employee_from_id_search, MissingPerson):
                candidate_employees.append(employee_from_id_search)
            else:
                return(MissingPerson)
        candidate_employees = [e for e in candidate_employees if e.location == 'Janelia Research Campus']
        if not candidate_employees:
            return(MissingPerson())
        guesses = []
        for employee in candidate_employees:
            employee_permuted_names = generate_name_permutations(employee.first_names, employee.middle_names, employee.last_names)
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
        print(f"Multiple high scoring matches found for {author.name}:")
        for guess in best_guess.winners:
            print(colored(f"{guess.name}, title: {guess.job_title}, CC: {guess.supOrgName}, {guess.email}", 'black', 'on_yellow'))
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
                    f"Employee best guess: {best_guess.name}, ID: {best_guess.id}, job title: {best_guess.job_title}, supOrgName: {best_guess.supOrgName}, email: {best_guess.email}, Confidence: {round(best_guess.score, ndigits = 3)}\n"
                    )
            # Do nothing
        else:
            print(inform_message)
            print(colored(
                f"Employee best guess: {best_guess.name}, ID: {best_guess.id}, job title: {best_guess.job_title}, supOrgName: {best_guess.supOrgName}, email: {best_guess.email}, Confidence: {round(best_guess.score, ndigits = 3)}",
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

def search_tricky_names(author_name):
    name = HumanName(author_name)
    namesearch_results = search_people_api(name.last, mode='name')
    if not isinstance(namesearch_results, MissingPerson):
        return(namesearch_results)
    namesearch_results = search_people_api(unidecode(name.last), mode='name')
    if not isinstance(namesearch_results, MissingPerson):
        return(namesearch_results)
    if '-' in name.last:
        namesearch_results = search_people_api(unidecode(name.last).split('-')[0], mode='name')
        if not isinstance(namesearch_results, MissingPerson):
            return(namesearch_results)
        namesearch_results = search_people_api(unidecode(name.last).split('-')[1], mode='name')
        if not isinstance(namesearch_results, MissingPerson):
            return(namesearch_results)
    namesearch_results = search_people_api(name.first, mode='name')
    if not isinstance(namesearch_results, MissingPerson):
        return(namesearch_results)
    return(MissingPerson())


def search_people_api(query, mode):
    response = None
    if mode not in {'name', 'id'}:
        raise ValueError("HHMI People API search mode must be either 'name' or 'id'.")
    if mode == 'name':
        response = JRC.call_people_by_name(query)
    elif mode == 'id':
        response = JRC.call_people_by_id(query)
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



# EXPERIMENTAL FUNCTIONS; WORK IN PROGRESS

def determine_authors_to_check(doi_record):
    all_authors = create_author_objects(doi_record)
    if not any([a.affiliations for a in all_authors]):
        return(all_authors)
    else:
        return([ a for a in all_authors if is_janelian(a, orcid_collection) ])



# def update_orcid_record_names_if_needed(author, employee, mongo_orcid_record):
#     if any(first_names_for_orcid_record(author, employee) not in mongo_orcid_record['given']):
#         #ADD NEW NAMES! TODO for Rob









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

    dois = [arg.DOI] if arg.DOI else []
    if arg.FILE:
        try:
            with open(arg.FILE, "r", encoding="ascii") as instream:
                for doi in instream.read().splitlines():
                    dois.append(doi)
        except Exception as err:
            print(f"Could not process {arg.FILE}")
            exit()

    for doi in dois:

        doi_record = get_doi_record(doi)
        print(f"{doi}: {doi_record['title'][0]}")
        authors_to_check = determine_authors_to_check(doi_record) # A list. If the paper has affiliations, the list is just those with janelia affiliations. Otherwise, all authors.
        revised_jrc_authors = []

        for author in authors_to_check:

            if author.orcid:
                mongo_orcid_record = doi_common.single_orcid_lookup(author.orcid, orcid_collection, 'orcid')
                if mongo_orcid_record:
                    if 'employeeId' in mongo_orcid_record:
                        employee = create_employee(mongo_orcid_record['employeeId'])
                        #update_orcid_record_names_if_needed(author, employee, mongo_orcid_record)
                        revised_jrc_authors.append(employee.id)
                        if arg.VERBOSE:
                            print( f"{author.name} is in our ORCID collection, with both an ORCID an employee ID. No action to take.\n" )
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
                                #TODO: ^ Add names to this
                                revised_jrc_authors.append(best_guess.id)

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
                                    #TODO: ^ Add names to this
                                    revised_jrc_authors.append(best_guess.id)
                            elif 'orcid' in employeeId_result: # Hopefully this will never get triggered
                                print(f"{author.name}'s ORCID is {author.orcid} on the paper, but it's {employeeId_result['orcid']} in our collection. Aborting program.")
                                sys.exit(1)
                        else:
                            print(f"{author.name} has an ORCID on this paper, and they are not in our collection.")
                            success_message = string.Template("Confirm you wish to create an ORCID record for $name, with both their employee ID and their ORCID")
                            confirm_proceed = confirm_action(success_message)
                            if confirm_proceed:
                                create_orcid_record(best_guess, orcid_collection, author)
                                revised_jrc_authors.append(best_guess.id)


            elif not author.orcid:
                inform_message = f"{author.name} does not have an ORCID on this paper."
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
                                    #update_orcid_record_names_if_needed(author, employee, mongo_orcid_record)
                                    revised_jrc_authors.append(best_guess.id)
                                else:
                                    print(f"{author.name} is in our collection with an employee ID only. No action to take.\n")
                                    #update_orcid_record_names_if_needed(author, employee, mongo_orcid_record)
                                    revised_jrc_authors.append(best_guess.id)
                            else:
                                print(f"There is no record in our collection for {author.name}.")
                                success_message = string.Template("Confirm you wish to create an ORCID record for $name with an employee ID only")
                                confirm_proceed = confirm_action(success_message)
                                if confirm_proceed:
                                    create_orcid_record(best_guess, orcid_collection, author)
                                    revised_jrc_authors.append(best_guess.id)
                elif isinstance(best_guess, Employee):
                    employeeId_result = doi_common.single_orcid_lookup(best_guess.id, orcid_collection, lookup_by='employeeId')
                    if employeeId_result:
                        if 'orcid' in employeeId_result:
                            if arg.VERBOSE == True:
                                print(f"{author.name} is in our collection with both an ORCID and an employee ID. No action to take.\n")
                            #update_orcid_record_names_if_needed(author, employee, mongo_orcid_record)
                            revised_jrc_authors.append(best_guess.id)
                        else:
                            if arg.VERBOSE == True:
                                print(f"{author.name} is in our collection with an employee ID only. No action to take.\n")
                            #update_orcid_record_names_if_needed(author, employee, mongo_orcid_record)
                            revised_jrc_authors.append(best_guess.id)
                    else:
                        proceed = evaluate_guess(author, best_guess, inform_message, verbose=arg.VERBOSE)
                        if proceed:
                            print(f"There is no record in our collection for {author.name}.")
                            success_message = string.Template("Confirm you wish to create an ORCID record for $name with an employee ID only")
                            confirm_proceed = confirm_action(success_message)
                            if confirm_proceed:
                                create_orcid_record(best_guess, orcid_collection, author)
                                revised_jrc_authors.append(best_guess.id)

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

    #doi='10.1101/2021.08.18.456004'
    #doi='10.7554/elife.80660'
    #doi='10.1101/2024.05.09.593460'
    #doi='10.1021/jacs.4c03092'
    #doi='10.1038/s41556-023-01154-4'

