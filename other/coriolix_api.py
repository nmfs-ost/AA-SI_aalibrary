"""This file contains the functions necessary for interacting with the Coriolix
API, to obtain Coriolix Metadata for cruises."""

import json
import requests

from pprint import pprint


class CoriolixAPI:
    api_root_url = "https://coriolix.savannah.skio.uga.edu/api/"

    def __init__(self):
        self.get_api_root()

    def get_api_root(self):
        self.api_root = json.loads(requests.get(url=self.api_root_url).content)
        pprint(self.api_root)


if __name__ == "__main__":
    coriolix = CoriolixAPI()
