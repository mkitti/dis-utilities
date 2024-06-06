''' get_citation.py
    Get the citation(s) for one or more DOIs through the DIS DB API.
    Citations are already in the 'Janelia Science News' format.
'''

import requests
import argparse

endpoint = "https://dis.int.janelia.org/citation/dis/" # the trailing slash is important
headers = {'Content-Type': 'application/json'}

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

def format_url_for_api(doi):
    return( endpoint + doi.replace("/", "%2F") ) # e.g. 10.1186/s12859-024-05732-7 becomes 10.1186%2Fs12859-024-05732-7

def get_citation(url):
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return(response.json()['data'])
    else:
        print(f"Failed to retrieve data. Status code: {response.status_code}.\n Error message: {response.reason}\n")
        if response.status_code == 404:
            print("A 404 error may indicate that the DOI is not in the database.")

def doi_to_citation(doi):
    base_doi = strip_doi_if_provided_as_url(doi)
    url = format_url_for_api(base_doi)
    citation = get_citation(url)
    return(citation)


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
    
    results = []
    if arg.DOI:
        results.append( doi_to_citation(arg.DOI) )
    elif arg.FILE:
        try:
            with open(arg.FILE, "r", encoding="ascii") as instream:
                for doi in instream.read().splitlines():
                    results.append( doi_to_citation(doi) )
        except Exception as err:
            print(f"Could not process {arg.FILE}")
            exit()

    for citation in sorted(results):
        print(citation)