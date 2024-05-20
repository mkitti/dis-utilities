''' doi_lib.py
    Library of routines for parsing and interpreting DOI records.
    Callable functions:
      get_publishing_date
'''


def get_author_list(rec):
    ''' Generate a text author list
        Keyword arguments:
          data: data record
        Returns:
          Text author list
    '''
    auth_list = []
    datacite = bool('DOI' not in rec)
    given = 'given'
    family = 'family'
    if datacite:
        rec = rec['attributes']
        given = 'givenName'
        family = 'familyName'
    author = rec['creators' if datacite else 'author']
    for auth in author:
        if given in auth:
            initials = auth[given].split()
            first = []
            for gvn in initials:
                first.append(gvn[0] + '.')
            full = ', '.join([auth[family], ' '.join(first)])
        elif family in auth:
            full = auth[family]
        else:
            full = auth['name']
        if 'ORCID' in auth:
            full = f"<a href='{auth['ORCID']}' target='_blank'>{full}</a>"
        auth_list.append(full)
    last = auth_list.pop()
    if last[-1] != '.':
        last += '.'
    if auth_list:
        return  ', '.join(auth_list) + ' & ' + last
    return None


def get_journal(rec):
    ''' Generate a journal name
        Keyword arguments:
          data: data record
        Returns:
          Journal name
    '''
    if 'DOI' in rec:
        # Crossref
        if 'short-container-title' in rec and rec['short-container-title']:
            journal = rec['short-container-title'][0]
        elif 'container-title' in rec and rec['container-title']:
            journal = rec['container-title'][0]
        elif 'institution' in rec:
            if isinstance(rec['institution'], list):
                journal = rec['institution'][0]['name']
            else:
                journal = rec['institution']['name']
        else:
            return None
        year = get_publishing_date(rec)
        if year == 'unknown':
            return None
        journal += '. ' + year.split('-')[0]
        if 'volume' in rec:
            journal += '; ' + rec['volume']
        if 'page' in rec:
            journal += ': ' + rec['page']
        else:
            journal += ': ' + rec['DOI'].split('/')[-1]
        return journal
    # DataCite
    rec = rec['attributes']
    year = get_publishing_date(rec)
    if year == 'unknown':
        return None
    if 'publisher' in rec and rec['publisher']:
        return f"{rec['publisher']}, {year.split('-')[0]}"


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


def get_title(rec):
    ''' Generate a title
        Keyword arguments:
          data: data record
        Returns:
          Title
    '''
    if 'DOI' in rec:
        # Crossref
        if 'title' in rec and rec['title'][0]:
            return rec['title'][0]
        return None
    # DataCite
    rec = rec['attributes']
    if 'titles' in rec and rec['titles'] and 'title' in rec['titles'][0]:
        return rec['titles'][0]['title']
    return None
