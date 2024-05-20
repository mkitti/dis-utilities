''' doi_lib.py
    Library of routines for parsing and interpreting DOI records.
    Callable functions:
      get_publishing_date
'''


def get_publishing_date(rec):
    """ Return the publication date
        published:
        published-print:
        published-online:
        posted:
        created:
        Keyword arguments:
          rec: Crossref or DataCite record
        Returns:
          Publication date
    """
    if 'DOI' in rec:
        # Crossref
        for sec in ('published', 'published-print', 'published-online', 'posted', 'created'):
            if sec in rec and 'date-parts' in rec[sec] and len(rec[sec]['date-parts'][0]) == 3:
                arr = rec[sec]['date-parts'][0]
                try:
                    return '-'.join([str(arr[0]), f"{arr[1]:02}", f"{arr[2]:02}"])
                except Exception as err:
                    raise err
    else:
        # DataCite
        if 'registered' in rec:
            return rec['registered'].split('T')[0]
    return 'unknown'
