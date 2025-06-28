"""This file contains the code to parse through the ICES API found here:
https://vocab.ices.dk/?ref=315
Specifically the `SHIPC` platform code which refers to ship names.
"""

import requests
from typing import List

# For pytests-sake
if __package__ is None or __package__ == "":
    # uses current directory visibility
    from utils.helpers import normalize_ship_name
else:
    # uses current package visibility
    from aalibrary.utils.helpers import normalize_ship_name


def get_all_ship_info() -> List:
    """Gets all of the ship's info from the following URL:
    https:/vocab.ices.dk/services/api/Code/7f9a91e1-fb57-464a-8eb0-697e4b0235b5


    Returns:
        List: A list with dicts of all the ships, including name, ices code,
            uuids and other fields.
    """

    response = requests.get(
        url=(
            "https://vocab.ices.dk/services/api/Code/"
            "7f9a91e1-fb57-464a-8eb0-697e4b0235b5"
        )
    )
    all_ship_info = response.json()

    return all_ship_info


def get_all_ices_ship_codes_and_names(
    normalize_ship_names: bool = False,
) -> dict:
    """Gets all of the ices ship codes and their corresponding names in a
    dictionary format. The keys are the ICES code, and the name is the value.

    Args:
        normalize_ship_names (bool, optional): Whether or not to format the
            ship name according to our own standards. Defaults to False.

    Returns:
        dict: A dict with all of the ICES ships. The keys are the ICES code,
            and the name is the value.
    """

    all_ship_info = get_all_ship_info()
    all_ship_codes_and_names = {}
    for ship_info in all_ship_info:
        all_ship_codes_and_names[ship_info["key"]] = ship_info["description"]

    if normalize_ship_names:
        all_ship_codes_and_names = {
            code: normalize_ship_name(name)
            for code, name in all_ship_codes_and_names.items()
        }

    return all_ship_codes_and_names


def get_all_ices_ship_names(normalize_ship_names: bool = False) -> List:
    """Gets all of the ICES ship names. You can normalize them to our standards
    if you wish.

    Args:
        normalize_ship_names (bool, optional): Whether or not to format the
            ship name according to our own standards. Defaults to False.

    Returns:
        List: A list containing strings of all of the ship names.
    """

    all_ship_info = get_all_ship_info()
    all_ship_names = []
    for ship_info in all_ship_info:
        # Here `ship_info` is a dict
        all_ship_names.append(ship_info["description"])
    if normalize_ship_names:
        all_ship_names = [
            normalize_ship_name(ship_name=ship_name)
            for ship_name in all_ship_names
        ]

    return all_ship_names


if __name__ == "__main__":
    all_ship_names = get_all_ices_ship_names(normalize_ship_names=True)
    for ship_name in all_ship_names:
        if "lasker" in ship_name.lower():
            print(ship_name)
