# dis-utilities:sync

## Utility programs for DIS system

| Name                   | Description                                                            |
| ---------------------- | ---------------------------------------------------------------------- |
| add_newsletter.py      | Add a newsletter date to a DOI                                         |
| edit_orcid.py          | Edit a record in the orcid collection                                  |
| find_missing_orcids.py | Find entries in the People system with groups (lab heads) but no ORCID |
| fix_jrc_author.py      | Add jrc_author field to DOIs that don't have it                        |
| fix_middle_names.py    | Expand given names in the orcid collection                             |
| get_citation.py        |                                                                        |
| name_match.py          |                                                                        |
| search_people.py       | Search for a name in the People system                                 |
| update_tags.py         | Modify tags (and optionally add newletter date) to DOIs                |

### Setup

First, create a Python virtual environment:

    cd utility/bin
    python3 -m venv my_venv

Enter the virtual environment and install necessary libraries:

    source my_venv/bin/activate
    my_venv/bin/pip install -r requirements.txt

Programs can now be run in the virtual environment:

    my_venv/bin/python3 add_newsletter.py --verbose
