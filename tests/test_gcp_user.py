"""aalibrary's own account lookup (utils.helpers.get_current_gcp_user_email).

gcloud's account setting can hold anything; Google's instructions read
`gcloud config set account ACCOUNT`, and run as written that put products in
derived_products/ACCOUNT/. No gcloud is run here: subprocess.run is faked.
"""

from __future__ import annotations

import subprocess

from aalibrary.utils import helpers


def _gcloud(monkeypatch, setting: str, accounts: list[str]) -> None:
    def fake_run(command, **kwargs):
        if command[:3] == ["gcloud", "config", "get-value"]:
            out = setting + "\n"
        else:
            out = "".join(a + "\n" for a in accounts)
        return subprocess.CompletedProcess(command, 0, stdout=out, stderr="")

    monkeypatch.setattr(helpers.subprocess, "run", fake_run)


def test_a_real_address_is_used_as_it_always_was(monkeypatch):
    _gcloud(monkeypatch, "michael.c.ryan@noaa.gov", ["other@noaa.gov"])
    assert helpers.get_current_gcp_user_email() == "michael.c.ryan@noaa.gov"


def test_the_placeholder_gives_way_to_the_one_signed_in_account(monkeypatch):
    _gcloud(monkeypatch, "ACCOUNT", ["colleague@noaa.gov"])
    assert helpers.get_current_gcp_user_email() == "colleague@noaa.gov"


def test_with_several_accounts_nothing_is_guessed(monkeypatch):
    _gcloud(monkeypatch, "ACCOUNT", ["a@noaa.gov", "b@noaa.gov"])
    assert helpers.get_current_gcp_user_email() == "ACCOUNT"   # as before


def test_addresses(monkeypatch):
    assert helpers._is_email_address("ada@noaa.gov")
    for value in ("ACCOUNT", "", "ada@", "@noaa.gov", "ada@noaa"):
        assert not helpers._is_email_address(value)
