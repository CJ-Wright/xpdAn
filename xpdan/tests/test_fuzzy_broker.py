from xpdan.fuzzybroker import fuzzy_search, super_fuzzy_search, beamtime_dates, \
    fuzzy_set_search
import pytest
from ..hfi import *
from redsky.savers import NPYSaver
from redsky.streamer import db_store_single_resource_single_file


@pytest.mark.parametrize(('search_str', 'target_str'),
                         [('chris', 'chris'), ('christ', 'chris'),
                          ('chry', 'chris'), ('tim', 'tim')])
def test_fuzzy_searches(exp_db, search_str, target_str):
    search_result = fuzzy_search(exp_db, 'pi_name', search_str)
    assert search_result[0]['start']['pi_name'] == target_str


@pytest.mark.parametrize(('search_str', 'target_str'),
                         [('chris', 'chris'), ('christ', 'chris'),
                          ('chry', 'chris'), ('tim', 'tim')])
def test_super_fuzzy_search(exp_db, search_str, target_str):
    search_result = super_fuzzy_search(exp_db, search_str)
    assert search_result[0]['start']['pi_name'] == target_str


def test_beamtime_dates_smoke(exp_db):
    beamtime_dates(exp_db)


@pytest.mark.parametrize(('search_str', 'target_str'),
                         [('chris', 'chris'), ('christ', 'chris'),
                          ('chry', 'chris'), ('tim', 'tim')])
def test_fuzzy_set_search(exp_db, search_str, target_str):
    res = fuzzy_set_search(exp_db, 'pi_name', search_str)
    assert res[0] == target_str
    assert len(res) == 2


@pytest.mark.parametrize(('search_str', 'target_str'),
                         [('chris', 'chris'), ('christ', 'chris'),
                          ('chry', 'chris'), ('tim', 'tim')])
def test_class_fuzzy_searches(fuzzdb, search_str, target_str):
    search_result = fuzzdb.fuzzy_search('pi_name', search_str)
    assert search_result[0]['start']['pi_name'] == target_str


@pytest.mark.parametrize(('search_str', 'target_str'),
                         [('chris', 'chris'), ('christ', 'chris'),
                          ('chry', 'chris'), ('tim', 'tim')])
def test_class_super_fuzzy_search(fuzzdb, search_str, target_str):
    search_result = fuzzdb.super_fuzzy_search(search_str)
    assert search_result[0]['start']['pi_name'] == target_str


def test_class_beamtime_dates_smoke(fuzzdb):
    fuzzdb.beamtime_dates()


@pytest.mark.parametrize(('search_str', 'target_str'),
                         [('chris', 'chris'), ('christ', 'chris'),
                          ('chry', 'chris'), ('tim', 'tim')])
def test_fuzzy_class_set_search(fuzzdb, search_str, target_str):
    res = fuzzdb.fuzzy_set_search('pi_name', search_str)
    assert res[0] == target_str
    assert len(res) == 2
