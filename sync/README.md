# dis-utilities:sync

## Programs for automated synchronization of dis database from external sources

| Name             | Description                                            |
| ---------------- | ------------------------------------------------------ |
| add_preprint.py  | Update preprint relations                              |
| group_search.py  | Find resources authored by group (lab) heads           |
| pull_arxiv.py    | Produce a list of aRxiv DOIs eligible for insertion    |
| pull_bioRxiv.py  | Produce a list of bioRxiv DOIs eligible for insertion  |
| pull_figshare.py | Produce a list of figshare DOIs eligible for insertion |
| update_dois.py   | Synchronize DOI information from Crossref/DataCite     |
| update_orcid.py  | Synchronize ORCID names and IDs                        |

### Setup

First, create a Python virtual environment:

    cd sync/bin
    python3 -m venv my_venv

Enter the virtual environment and install necessary libraries:

    source my_venv/bin/activate
    venv/bin/pip install -r requirements.txt

Programs can now be run in the virtual environment:

    venv/bin/python3 update_dois.py --verbose

## ORCID

Data from ORCID is synchronized to the orcid collection in the dis MongoDB database. Names from ORCID entries that have one of the following affiliations are eligible for inclusion:

- "Janelia Research Campus"
- "Janelia Farm Research Campus"
- Janelia identifier from Research Organization Registry (ROR)

Author names are also pulled from author records in the dois collection in the dis database. Author names must have an ORCID specified in the resource's author record, and an affiliation containing "Janelia". After a list of Janelia authors is populated, correlate each entry with the People database. If the author name is found, HHMI data (username, HHMI affiliations, etc.) will be added to the record. Also added will be:

- Group leadership (reserved for Janelia Group Leaders)
- Group membership (HHMI affiliations)
- Name combinations (with/without middle name, punctuation, and/or accents)

Processed records will be inserted/updated. Janelia employees from the orcid collection are then backchecked against the HHMI People database. If the user is no longer in the People database, their orcid record is then ginev alumni status.

The update_orcid.py program is run every night on Jenkins.
