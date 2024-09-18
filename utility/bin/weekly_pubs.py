import os
import argparse
import subprocess
from collections.abc import Iterable

sync_bin_path = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'sync', 'bin'))

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


def doi_source(ARG):
    if ARG.DOI:
        return( ['--doi', ARG.DOI] )
    elif ARG.FILE:
        return(['--file', ARG.FILE])
    else:
        sys.exit("ERROR: Neither --doi nor --file provided.")

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


def create_command(script, ARG): # will produce a list like, e.g. ['python3', 'update_dois.py', '--doi' '10.1038/s41593-024-01738-9', '--verbose']
    return(
        list(flatten( ['python3', script, doi_source(ARG), verbose(ARG), write(ARG)] ))
    )


# use get_doi_record to check whether its in db alreday, if it is, don't add
#can use doi/<doi> endpoint, check 'source' key under the 'rest' key. if 'source' is 'mongo', don't add
subprocess.call(create_command(f'{sync_bin_path}/update_dois.py', ARG))

subprocess.call(create_command('name_match.py', ARG))

subprocess.call(create_command('update_tags.py', ARG))

subprocess.call(list(flatten( ['python3', 'get_citation.py', doi_source(ARG)] ))) 

