from pyxdameraulevenshtein import \
    normalized_damerau_levenshtein_distance as ndld
from databroker.broker import _munge_time
from pprint import pprint
import pytz
from heapq import heapify, heappushpop


def get_from_dict(data_dict, map_list):
    """ Get a value from a nested dictionary, given a list of keys

    Parameters
    ----------
    data_dict: dict
        The dictionary to be queried
    map_list: list of str
        A list of strings, each string is one level lower than the previous

    Returns
    -------
    object:
        The the value from the dict

    """
    for k in map_list:
        data_dict = data_dict[k]
    return data_dict


def nested_dict_values(d):
    """Yield all string values inside a nested dictionary

    Parameters
    ----------
    d: dict
        The dictionary to be unpacked

    Yields
    -------
    str:
        The string value inside the dictionary

    """
    for v in d.values():
        if isinstance(v, dict):
            yield from nested_dict_values(v)
        else:
            if isinstance(v, str):
                yield v
            else:
                yield None


def fuzzy_search(db, keys, search_string, size=100):
    """Fuzzy search a databroker for given keys

    Parameters
    ----------
    db: databroker.DataBroker instance
        The databroker to be searched
    keys: list of str
        The list of strings to be accessed
    search_string: str
        The string to be searched for
    size: int or 'all', optional
        The number of results to be returned, if 'all' all are returned.
         Defaults to 100 results

    Returns
    -------
    list:
        A list

    """
    heap = [(-1, -1, -1)] * size  # ndld can't return less than 0
    heapify(heap)
    if isinstance(keys, list):
        for h in db():
            # prioritize recent documents
            heappushpop(heap, (1. - ndld(get_from_dict(h['start'], keys),
                                         search_string),
                               h['start']['time'] * -1, h))
    else:
        for h in db():
            heappushpop(heap, (1. - ndld(h['start'][keys], search_string),
                               h['start']['time'] * -1, h))
    heap.sort()
    heap.reverse()
    return [g[-1] for g in heap if g[0] >= 0.]


def super_fuzzy_search(db, search_string, size=100):
    """Fuzzy search a databroker

    Parameters
    ----------
    db: databroker.DataBroker instance
        The databroker to be searched
    search_string: str
        The string to be searched for
    size: int, optional
        The number of results to be returned.
         Defaults to 100 results

    Returns
    -------
    list:
        A list

    """
    heap = [(-1, -1, -1)] * size  # ndld can't return less than 0
    heapify(heap)
    for h in db():
        internal_scores = [1. - ndld(v, search_string) for v in
                           nested_dict_values(h['start']) if v is not None]
        heappushpop(heap, (max(internal_scores), h['start']['time'] * -1, h))
    heap.sort()
    heap.reverse()
    return [g[-1] for g in heap if g[0] != -1]


def beamtime_dates(db, keys=('beamtime_uid', 'bt_safN',
                             'facility', 'beamline'),
                   beamtime_key='beamtime_uid',
                   print=True):
    """Get info for each beamtime

    Parameters
    ----------
    db: databroker instance
        The databroker to be searched
    keys: iterable of str
        The keys to be included in the return
    beamtime_key: str
        The key for the unique beamtime key
    print: bool
        If true prints the information

    Returns
    -------
    list of dicts:
        The list of beamtimes and their associated information
    """
    hdrs = db()
    bts = set([h['start'][beamtime_key] for h in hdrs])
    returns = []
    for s in bts:
        hdrs = db(**{beamtime_key: s})
        start_hdr = hdrs[0]
        stop_hdr = hdrs[-1]
        info = {k: start_hdr[k] for k in keys if k in start_hdr.keys()}
        info.update({'start_time': _munge_time(start_hdr['start']['time'],
                                               pytz.timezone('US/Eastern')),
                     'stop_time': _munge_time(stop_hdr['start']['time'],
                                              pytz.timezone('US/Eastern'))})
        returns.append(info)
    if print:
        pprint(returns)
    return returns


def fuzzy_set_search(db, key, search_string, size=100):
    """Return the most similar set of values to the search string.

    Parameters
    ----------
    db: databroker.DataBroker instance
        The databroker to be searched
    key: list of str
        The list of strings to be accessed
    search_string: str
        The string to be searched for
    size: int, optional
        The number of results to be returned.
         Defaults to 100 results

    Returns
    -------
    list:
        A list

    Examples
    --------
    >>> db = Broker(...) # Contains runs from Bob, Alice, Bob, and Eve
    >>> fuzzy_set_search(db, 'bt_piLast', 'Bob')
    ['Bob', 'Alice', 'Eve']
    """
    heap = [(-1, -1)] * size  # ndld can't return less than 0
    heapify(heap)
    values = set([h['start'][key] for h in db()])
    for v in values:
        heappushpop(heap, (1. - ndld(v, search_string), v))
    heap.sort()
    heap.reverse()
    return [g[-1] for g in heap if g[0] >= 0.]