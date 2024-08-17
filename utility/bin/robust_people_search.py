from collections.abc import Iterable
from nameparser import HumanName
from rapidfuzz import fuzz, utils
from unidecode import unidecode
import jrc_common.jrc_common as JRC

#TODO: Integrate this script into name_match.py

def name_search(first, last):
    search_results1 = search_people_api(first, mode='name')
    search_results2 = search_people_api(last, mode='name')
    if search_results1 and search_results2:
        return( process_search_results(search_results1, search_results2) )
    else:
        return(None)
        

def process_search_results(list1, list2): # We require that the same employeeId appear in both the first and last name searches
    employee_ids_list1 = {item['employeeId'] for item in list1}
    employee_ids_list2 = {item['employeeId'] for item in list2}
    common_ids = list(employee_ids_list1.intersection(employee_ids_list2))
    if common_ids:
        return(common_ids)
    else:
        return(None)


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

name = HumanName('Daniel Bushey') # Debugging: Norma C. Pérez Rosas,  Miguel Angel Núñez Ochoa, David Clapham (he has two people entries)
basic = name_search(name.first, name.last)
stripped = name_search(unidecode(name.first), unidecode(name.last)) # decode accents and other special characters
hyphen_split1 = name_search(name.first, name.last.split('-')[0]) if '-' in name.last else None
hyphen_split2 = name_search(name.first, name.last.split('-')[1]) if '-' in name.last else None
strp_hyph1 = name_search(unidecode(name.first), unidecode(name.last.split('-')[0])) if '-' in name.last else None
strp_hyph2 = name_search(unidecode(name.first), unidecode(name.last.split('-')[1])) if '-' in name.last else None
two_middle_names1 = name_search(name.first, name.middle.split(' ')[0]) if len(name.middle.split())==2 else None
two_middle_names2 = name_search(name.first, name.middle.split(' ')[1]) if len(name.middle.split())==2 else None
strp_middle1 = name_search(unidecode(name.first), unidecode(name.middle.split()[0])) if len(name.middle.split())==2 else None
strp_middle2 = name_search(unidecode(name.first), unidecode(name.middle.split()[1])) if len(name.middle.split())==2 else None

all_results = [basic, stripped, hyphen_split1, hyphen_split2, strp_hyph1, strp_hyph2, two_middle_names1, two_middle_names2, strp_middle1, strp_middle2]
candidates = [id for id in list(set(flatten(all_results))) if id is not None]
print(candidates)






