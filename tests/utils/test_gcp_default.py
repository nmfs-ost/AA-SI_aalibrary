"""`import aalibrary` defaults to production but keeps a project it is given."""

from __future__ import annotations

import os
import subprocess
import sys

import pytest

from aalibrary import config


@pytest.fixture()
def clean_env(monkeypatch):
    for name in ("AALIBRARY_GCP_PROJECT_ID", "AALIBRARY_GCP_BUCKET_NAME"):
        monkeypatch.delenv(name, raising=False)


def test_nothing_set_means_production(clean_env):
    config.use_gcp_default()
    assert os.environ["AALIBRARY_GCP_PROJECT_ID"] == config.GCP_PROD_PROJECT_ID
    assert os.environ["AALIBRARY_GCP_BUCKET_NAME"] == config.GCP_PROD_BUCKET_NAME


def test_a_given_project_is_kept_with_its_bucket(clean_env, monkeypatch):
    monkeypatch.setenv("AALIBRARY_GCP_PROJECT_ID", config.GCP_DEV_PROJECT_ID)
    config.use_gcp_default()
    assert os.environ["AALIBRARY_GCP_PROJECT_ID"] == config.GCP_DEV_PROJECT_ID
    assert os.environ["AALIBRARY_GCP_BUCKET_NAME"] == config.GCP_DEV_BUCKET_NAME
    monkeypatch.setenv("AALIBRARY_GCP_BUCKET_NAME", "custom-results")
    config.use_gcp_default()
    assert os.environ["AALIBRARY_GCP_BUCKET_NAME"] == "custom-results"


def test_importing_aalibrary_keeps_the_callers_project():
    """In a fresh process, as a console tool started by the Workbench is."""
    env = {**os.environ, "AALIBRARY_GCP_PROJECT_ID": "someone-else-7"}
    env.pop("AALIBRARY_GCP_BUCKET_NAME", None)
    code = (
        "import os, aalibrary; "
        "print(os.environ['AALIBRARY_GCP_PROJECT_ID'], "
        "os.environ['AALIBRARY_GCP_BUCKET_NAME'])"
    )
    out = subprocess.run(
        [sys.executable, "-c", code], env=env, capture_output=True, text=True,
        check=True,
    ).stdout.split()
    assert out == ["someone-else-7", "someone-else-7-data"]
