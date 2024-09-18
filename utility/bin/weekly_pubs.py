''' weekly_pubs.py
    Run all the librarian's scripts for the weekly pipeline, in order.
    This script should live in utility/bin. It expects update_dois.py to be in sync/bin.
    IMPORTANT: I'm keeping the --write flag for consistency with all our other scripts, 
    but it doesn't really make sense to NOT include the --write flag in this script.
    And new DOIs won't be added to the database, so you'll get errors in the downstream scripts.
'''

import os
import sys
import argparse
import requests
import copy
import subprocess
from collections.abc import Iterable
import jrc_common.jrc_common as JRC
from operator import attrgetter
from termcolor import colored



# Functions to pass command line args to subsequent python scripts

def create_command(script, ARG): # will produce a list like, e.g. ['python3', 'update_dois.py', '--doi' '10.1038/s41593-024-01738-9', '--verbose']
    return(
        list(flatten( ['python3', script, doi_source(ARG), verbose(ARG), write(ARG)] ))
    )


def doi_source(ARG):
    if ARG.DOI:
        return( ['--doi', ARG.DOI] )
    elif ARG.FILE:
        return(['--file', ARG.FILE])

def verbose(ARG):
    if ARG.VERBOSE:
        return('--verbose')
    else:
        return([])

def write(ARG):
    if ARG.WRITE:
        return('--write')
    else:
        return([])

def flatten(xs): # https://stackoverflow.com/questions/2158395/flatten-an-irregular-arbitrarily-nested-list-of-lists
    for x in xs:
        if isinstance(x, Iterable) and not isinstance(x, (str, bytes)):
            yield from flatten(x)
        else:
            yield x



# Functions to handle DOIs already in the database

def copy_arg_for_sync(ARG):
    arg_copy = copy.deepcopy(ARG)
    dois = get_dois_from_commandline(ARG)
    dois_to_sync = [ d for d in dois if not already_in_db(d) ]
    if ARG.DOI:
        arg_copy.DOI = dois_to_sync
    elif ARG.FILE:
        with open('to_sync.txt', 'w') as outF:
            outF.write("\n".join(dois_to_sync) )
        arg_copy.FILE = 'to_sync.txt'
    return(arg_copy)

def get_dois_from_commandline(ARG):
    dois = [ARG.DOI.lower()] if ARG.DOI else [] # .lower() because our collection is case-sensitive
    if ARG.FILE:
        try:
            with open(ARG.FILE, "r", encoding="ascii") as instream:
                for doi in instream.read().splitlines():
                    dois.append(doi.strip().lower())
        except Exception as err:
            print(f"Could not process {ARG.FILE}")
            exit()
    return(dois)


# Functions to query the API

def already_in_db(doi):
    if get_rest_info(doi)["source"] == "mongo":
        return(True)
    else:
        return(False)

def get_rest_info(doi):
    rest = JRC.get_config("rest_services")
    url_base = attrgetter("dis.url")(rest)
    url = f'{url_base}doi/{replace_slashes_in_doi(strip_doi_if_provided_as_url(doi))}'
    response = get_request(url)
    return( response['rest'] ) 

def replace_slashes_in_doi(doi):
    return( doi.replace("/", "%2F") ) # e.g. 10.1186/s12859-024-05732-7 becomes 10.1186%2Fs12859-024-05732-7

def strip_doi_if_provided_as_url(doi, substring=".org/10.", doi_index_in_substring = 5):
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

def get_request(url):
    headers = { 'Content-Type': 'application/json' }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return(response.json())
    else:
        print(f"There was an error with the API GET request. Status code: {response.status_code}.\n Error message: {response.reason}")
        sys.exit(1)





# -----------------------------------------------------------------------------

if __name__ == '__main__':

    PARSER = argparse.ArgumentParser(
    description = "Run the weekly pipeline for one or more DOIs: add DOI(s) to database, curate authors and tags, and print citation(s).")
    MUEXGROUP = PARSER.add_mutually_exclusive_group(required=True)
    MUEXGROUP.add_argument('--doi', dest='DOI', action='store',
                            help='Single DOI to process')
    MUEXGROUP.add_argument('--file', dest='FILE', action='store',
                            help='File of DOIs to process')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Write results to database. If --write is missing, no changes to the database will be made.')

    ARG = PARSER.parse_args()

# If we add a DOI to the database that is already in there, the load source will be set to 'Manual' in the metadata, 
# which is misleading, so we don't want to add DOIs that are already in there.
# We can use the API to check whether the DOI is already in the database.
    sync_bin_path = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'sync', 'bin'))
    arg_copy = copy_arg_for_sync(ARG)
    if not arg_copy.DOI and not arg_copy.FILE:
        print(colored(
                ("WARNING: No DOIs to add to database. Skipping sync."), "yellow"
            ))
    else:
        subprocess.call(create_command(f'{sync_bin_path}/update_dois.py', arg_copy))

    subprocess.call(create_command('name_match.py', ARG))

    subprocess.call(create_command('update_tags.py', ARG))

    subprocess.call(list(flatten( ['python3', 'get_citation.py', doi_source(ARG)] ))) 

