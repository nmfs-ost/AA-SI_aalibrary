"""For testing ingestion."""

import pytest

from app import ingestion


class TestNCEIIngestion():
    """A class which tests various ingestion functionality of the API."""

    def setup_class(cls):
        ...

    def test_one(self):
        print("Test one done.")

    def test_force_download_from_NCEI():
        ...
    
    def test_download_raw_idx_from_GCP():
        ...
    
    def test_parse_correct_gcp_location():
        ...
    
    
