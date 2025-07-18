"""This file contains the code to parse through the ICES API found here:
https://vocab.ices.dk/?ref=315
Specifically the `SHIPC` platform code which refers to ship names.
"""

import requests
from typing import List
from difflib import get_close_matches

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

    print("HELLO!")
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


def get_ices_code_from_ship_name(
    ship_name: str = "", is_normalized: bool = False
) -> str:
    """Gets the ICES Code for a ship given a ship's name.

    Args:
        ship_name (str, optional): The ship name string. Defaults to "".
        is_normalized (bool, optional): Whether or not the ship name is already
            normalized according to aalibrary standards. Defaults to False.

    Returns:
        str: The ICES Code if one has been found. Empty string if it has not.
    """

    # Get all of the ship codes and names.
    all_codes_and_names = get_all_ices_ship_codes_and_names(
        normalize_ship_names=is_normalized
    )
    # Reverse it to make the ship names the keys.
    all_codes_and_names = {v: k for k, v in all_codes_and_names.items()}
    valid_ICES_ship_names = list(all_codes_and_names.keys())
    # Try to find the correct ICES code based on the ship name.
    try:
        return all_codes_and_names[ship_name]
    except KeyError:
        # Here the ship name does not exactly match any in the ICES DB.
        # Check for spell check using custom list
        spell_check_list = get_close_matches(
            ship_name, valid_ICES_ship_names, n=3, cutoff=0.6
        )
        if len(spell_check_list) > 0:
            print(
                f"This `ship_name` {ship_name} does not"
                " exist in the ICES database. Did you mean one of the"
                f" following?\n{spell_check_list}"
            )
        else:
            print(
                f"This `ship_name` {ship_name} does not"
                " exist in the ICES database. A close match could not be "
                "found."
            )
        return ""


if __name__ == "__main__":
    # all_ship_names = get_all_ices_ship_names(normalize_ship_names=True)
    # for ship_name in all_ship_names:
    #     if "lasker" in ship_name.lower():
    #         print(ship_name)
    print(get_ices_code_from_ship_name("Reuben_Lasker", is_normalized=True))
