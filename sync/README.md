# dis-utilities:sync

## Programs for automated synchronization of dis database with external sources

| Name            | Description                                        |
| --------------- | -------------------------------------------------- |
| group_search.py | Find resources authored by group (lab) heads       |
| update_dois.py  | Synchronize DOI information from Crossref/DataCite |
| update_orcid.py | Synchronize ORCID names and IDs                    |

### Setup

First, create a Python virtual environment:

    cd utility/bin
    python3 -m venv my_venv

Enter the virtual environment and install necessary libraries:

    source my_venv/bin/activate
    venv/bin/pip install -r requirements.txt
