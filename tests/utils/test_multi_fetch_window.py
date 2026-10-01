"""The time-window SQL of the multi-fetch YAML parser (no BigQuery needed)."""

import datetime as dt

import pytest

mf = pytest.importorskip("aalibrary.utils.multi_fetch_yaml_parser")


def _clause(windows):
    request = {"vessel": "Henry_B._Bigelow", "survey": "HB1603",
               "instrument": "EK60", "time-windows": windows}
    return mf.RequestParser(request_dict=request, gcp_project_id="p").sql_conditions_clause


def test_multi_day_window_is_one_interval():
    sql = _clause([{"start-date": "2016-07-03", "start-time": "22:00:00",
                    "end-date": "2016-07-04", "end-time": "02:00:00"}])
    assert ">= '2016-07-03 22:00:00'" in sql
    assert "<= '2016-07-04 02:00:00'" in sql
    # The old per-part comparisons (which made this window empty) are gone.
    assert "RIGHT(file_datetime,8) >=" not in sql


def test_bare_end_date_means_the_whole_day_and_windows_are_ored():
    sql = _clause([{"start-date": "2016-07-03", "end-date": "2016-07-03"},
                   {"start-date": "2016-07-05", "start-time": "06:00:00",
                    "end-date": "2016-07-05", "end-time": "00:00:00"}])
    assert "<= '2016-07-03 23:59:59'" in sql and "<= '2016-07-05 23:59:59'" in sql
    assert ">= '2016-07-03 00:00:00'" in sql and ">= '2016-07-05 06:00:00'" in sql
    assert "\nOR (" in sql


def test_yaml_typed_values_and_quotes():
    sql = _clause([{"start-date": dt.date(2016, 7, 3), "start-time": 21600,
                    "end-date": "2016-07-03'--", "end-time": "12:00:00"}])
    assert ">= '2016-07-03 06:00:00'" in sql
    assert "2016-07-03''--" in sql            # a quote cannot close the literal
