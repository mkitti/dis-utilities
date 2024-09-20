''' get_citation.py
    Get the citation(s) for one or more DOIs through the DIS DB API.
    Citations are already in the 'Janelia Science News' format.
'''

import requests
import argparse
import sys
import jrc_common.jrc_common as JRC
from operator import attrgetter
from termcolor import colored


class Citation:
    def __init__(self, citation=None, preprint=None):
        self.citation = citation # a string
        self.preprint = preprint # If the DOI is a journal article, this is a list. else None

class SimpleItem:
    def __init__(self, doi=None, item_type=None):
        self.doi = doi
        self.item_type = item_type

def create_simple_item(doi_record):
    item = SimpleItem(
        doi = doi_record['doi'] if 'doi' in doi_record else doi_record['DOI'], # In DIS DB, DataCite only has DOI; Crossref has both doi and DOI
        item_type = get_type(doi_record)
    )
    return(item)

def create_citation(doi):
    rest = JRC.get_config("rest_services")
    url_base = attrgetter("dis.url")(rest)
    response = get_request(f"{url_base}citation/dis/{replace_slashes_in_doi(strip_doi_if_provided_as_url(doi))}")
    if response:
        if 'jrc_preprint' in response:
            doi_record = get_doi_record(doi)
            item = create_simple_item(doi_record)
            if item.item_type == 'Journal article':
                return( Citation(citation=response['data'], preprint=response['jrc_preprint']) )
        return(Citation(citation=response['data']))
    else:
        print(colored( (f'WARNING: Unable to retrieve a citation for {doi}'), "yellow" ))
        return(None)

def get_doi_record(doi):
    rest = JRC.get_config("rest_services")
    url_base = attrgetter("dis.url")(rest)
    url = f'{url_base}doi/{replace_slashes_in_doi(strip_doi_if_provided_as_url(doi))}'
    response = get_request(url)
    return( response['data'] ) 


def get_type(doi_record):
    if 'type' in doi_record: # crossref
        if doi_record['type'] == 'journal-article':
            return('Journal article')
        if doi_record['type'] == 'posted-content':
            if doi_record['subtype'] == 'preprint':
                return('Preprint')
    else: # datacite
        return(doi_record['types']['resourceTypeGeneral'])

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
        print(f"ERROR: GET request status code: {response.status_code}. Error message: {response.reason}")
        # sys.exit(1)
        return(None)

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Turn a list of DOIs into a list of citations in Janelia Science News format.")
    muexgroup = parser.add_mutually_exclusive_group(required=True)
    muexgroup.add_argument('--doi', dest='DOI', action='store',
                         help='Produce a citation from a single DOI.')
    muexgroup.add_argument('--file', dest='FILE', action='store',
                         help='Produce a citation from a file containing one or more DOIs.')
    
    arg = parser.parse_args()
    
    citations = []
    if arg.DOI:
        citations.append( create_citation(arg.DOI) )
    elif arg.FILE:
        try:
            with open(arg.FILE, "r", encoding="ascii") as instream:
                for doi in instream.read().splitlines():
                    citations.append( create_citation(doi) )
        except:
            print(f"Could not process {arg.FILE}")
            raise ImportError
    
    citations = [c for c in citations if c is not None]
    for citation in sorted(citations, key=lambda c: c.citation):        
        if citation.preprint:
            print(f"{citation.citation}")
            for n in range(len(citation.preprint)):
                if n == len(citation.preprint)-1:
                    print(f"Preprint: {citation.preprint[n]}\n")
                else:
                    print(f"Preprint: {citation.preprint[n]}")

            # print(f"{citation.citation}")
            # for pp in citation.preprint:
            #     print(f"Preprint: {pp}")
            # print("\n")
        else:
            print(f"{citation.citation}\n")

# debugging: 10.7554/elife.90523 is a journal article with multiple preprints
