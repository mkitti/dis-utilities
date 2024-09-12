# dis-utilities:sync

## Programs for automated synchronization of dis database from external sources

| Name                 | Description                                            |
| -------------------- | ------------------------------------------------------ |
| email_authors.py     | Email information on newly-curated DOIs to authors     |
| group_search.py      | Find resources authored by group (lab) heads           |
| pull_arxiv.py        | Produce a list of aRxiv DOIs eligible for insertion    |
| pull_bioRxiv.py      | Produce a list of bioRxiv DOIs eligible for insertion  |
| pull_figshare.py     | Produce a list of figshare DOIs eligible for insertion |
| pull_oa.py           | Produce a list of OA DOIs eligible for insertion       |
| update_dois.py       | Synchronize DOI information from Crossref/DataCite     |
| update_orcid.py      | Synchronize ORCID names and IDs                        |
| update_preprints.py  | Update preprint relations                              |

### Development setup

First, create a Python virtual environment:

    cd sync/bin
    python3 -m venv my_venv

Enter the virtual environment and install necessary libraries:

    source my_venv/bin/activate
    my_venv/bin/pip install -r requirements.txt

Programs can now be run in the virtual environment:

    my_venv/bin/python3 update_dois.py --verbose

### Making latest codebase available to Jenkins

Simply update the VERSION in update_gcr.sh then run it. This requires access to gcloud and
the "sandbox-220614" project space.


### Dependencies

1. The libraries specified in requirements.txt need to be installed.
2. The [Configuration system](https://github.com/JaneliaSciComp/configurator) must be accessible. The following configurations are used:
    - databases
    - dis
    - dois
    - em_dois
    - releases
    - rest_services
3. The following keys must be present in the run environment:
    - CONFIG_SERVER_URL: abase URL for Configuration system
    - PEOPLE_API_KEY: API key for HHMI People system
4. The *config.ini* file must have the latest URL bases and suffixes for Crossref, DataCite, FigShare, and ORCID.

## ORCID

### Processing

Data from ORCID is synchronized to the orcid collection in the dis MongoDB database. Names from ORCID entries that have one of the following affiliations are eligible for inclusion:

- "Janelia Research Campus"
- "Janelia Farm Research Campus"
- Janelia identifier from Research Organization Registry (ROR)

Author names are also pulled from author records in the dois collection in the dis database. Author names must have an ORCID specified in the resource's author record, and an affiliation containing "Janelia". After a list of Janelia authors is populated, correlate each entry with the People database. If the author name is found, HHMI data (username, HHMI affiliations, etc.) will be added to the record. Also added will be:

- Group leadership (reserved for Janelia Group Leaders)
- Group membership (HHMI affiliations)
- Name combinations (with/without middle name, punctuation, and/or accents)

Processed records will be inserted/updated. Janelia employees from the orcid
collection are then backchecked against the HHMI People database. If the user
is no longer in the People database, their orcid record is then given alumni
status. Results of a typical run are below:
```
    Records read from MongoDB:dois: 753
    Records read from ORCID:        561
    ORCIDs inserted:                0
    ORCIDs updated:                 657
    ORCIDs set to alumni:           0
```
### Running in production

The *update_orcid.py* program is run every morning on [Jenkins](https://jenkins.int.janelia.org/view/DIS/job/DIS-sync-dis-update_orcid/).

## DOIs

### Crossref/DataCite processing 
DOIs are synchronized from Crossref and DataCite to the dois collection in the
dis MongoDB database. DOIs are also drawn from the following sources (in the
event than an update is needed):
- FLYF2 database
- ALPS releases
- EM datasets
- MongoDB dois collection
As noted above, DOIs are also from from Crossref and DataCite:
- Crossref: DOIs where at least one author has an affiliation containing "Janelia"
- DataCite: DOIs where at least one author has an affiliation containing "Janeli
a", and DOIs starting with 10.25378
New DOIs are inserted, and DOIs that have been updated (according to the
record from Crossref or DataCite) after the stored update date (in the dois
collection) are reprocessed. Results of a typical run are below:
```
    DOIs fetched from Crossref:      1,278
    DOIs fetched from DataCite:      3,231
    DOIs specified:                  6,725
    DOIs found in Crossref:          3,416
    DOIs found in DataCite:          3,282
    DOIs with no author:             0
    DOIs not found:                  0
    Duplicate DOIs:                  29
    DOIs not needing updates:        6,693
    DOIs inserted:                   3
    DOIs updated:                    2
    Elapsed time: 0:05:51.688400
    DOI calls to Crossref: 2,140
    DOI calls to DataCite: 51
```
Any newly-inserted DOIs are emailed to Virginia and Rob.

### Running in production

The *update_dois.py* program is run every morning on [Jenkins](https://jenkins.int.janelia.org/view/DIS/job/DIS-sync-dis-update_dois/).

### FlyCore processing 
A list of DOIs is retrieved from the FLYF2 database using the
[FlyCore responder](https://informatics-prod.int.janelia.org/cgi-bin/flycore_responder.cgi?request=doilist).
These are then added to the doi_data table in the FlyBoy database. Results of
a typical run are below:
```
    DOIs specified:                  110
    DOIs found in Crossref:          105
    DOIs found in DataCite:          1
    DOIs with no author:             0
    DOIs not found:                  4
    Duplicate DOIs:                  0
    DOIs not needing updates:        106
    DOIs found in FlyBoy:            105
    DOIs inserted/updated in FlyBoy: 0
    DOIs deleted from FlyBoy:        0
    DOIs inserted:                   0
    DOIs updated:                    0
    Elapsed time: 0:00:21.078572
    DOI calls to Crossref: 105
    DOI calls to DataCite: 1
```
### Running in production

The *update_dois.py* program is run every morning on [Jenkins](https://jenkins.int.janelia.org/view/DIS/job/DIS-sync-flyboy-update_dois/).

### arXiv
Potentially relevant DOIs are found in the arXiv repository:
- At least one author has an affiliation containing "Janelia" or an ORCID in the orcid collection
New DOIs are inserted. Results of a typical run are below:
```
    DOIs read from arXiv:            10
    DOIs already in database:        4
    DOIs in DataCite (asserted):     0
    DOIs not in DataCite:            0
    DOIs with no Janelian authors:   5
    DOIs ready for processing:       0
    DOIs requiring review:           1
```
Any newly-inserted DOIs are emailed to Virginia and Rob.

### Running in production

The *pull_arxiv.py* program is run every morning on [Jenkins](https://jenkins.int.janelia.org/view/DIS/job/DIS-sync-pull_arxiv/).

### bioRxiv
Potentially relevant DOIs are found in the bioRxiv repository:
- At least one author has an affiliation containing "Janelia" or an ORCID in the orcid collection
New DOIs are inserted. Results of a typical run are below:
```
    DOIs read from bioRxiv:          1,189
    DOIs already in database:        1
    DOIs not in Crossref (asserted): 0
    DOIs not in Crossref:            0
    DOIs with no Janelian authors:   1,037
    DOIs ready for processing:       0
    DOIs requiring review:           4
```
Any newly-inserted DOIs are emailed to Virginia and Rob.

### Running in production

The *pull_biorxiv.py* program is run every morning on [Jenkins](https://jenkins.int.janelia.org/view/DIS/job/DIS-sync-pull_biorxiv/).

### figshare
Relevant DOIs are found in the figshare repository:
- Institution = 295
New DOIs are inserted. Results of a typical run are below:
```
    DOIs read from figshare:   1,563
    Janelia DOIs:              1,547
    DOIs already in database:  1,563
    DOIs ready for processing: 0
```
Any newly-inserted DOIs are emailed to Virginia and Rob.

### Running in production

The *pull_figshare.py* program is run every morning on [Jenkins](https://jenkins.int.janelia.org/view/DIS/job/DIS-sync-pull_figshare/).

### OA
DOIs are found in the OA repository. **All** DOIs from this repository are relevant. There will be some without current 
Janelia authors; these are alumni.
New DOIs are inserted. Results of a typical run are below:
```
    DOIs read from OA:               2,470
    DOIs already in database:        2,277
    DOIs not in Crossref (asserted): 0
    DOIs not in Crossref:            0
    DOIs with no Janelian authors:   193
    DOIs ready for processing:       193
```
Any newly-inserted DOIs are emailed to Virginia and Rob.

### Running in production

The *pull_oa.py* program is run every Friday morning on [Jenkins](https://jenkins.int.janelia.org/view/DIS/job/DIS-sync-pull_oa/).
