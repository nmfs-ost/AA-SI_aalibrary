from aalibrary.ices_ship_names import get_all_ices_ship_names
from aalibrary.utils.ncei_utils import (
    get_all_survey_names_from_a_ship,
    get_all_echosounders_in_a_survey,
)
from InquirerPy import inquirer


def main():

    mode = inquirer.select(
        message="Select option",
        choices=["Search by vessel"],
        default="Search by vessel",
    ).execute()

    if mode == "Search by vessel":
        ship_name = inquirer.fuzzy(
            message="Select a vessel:",
            choices=get_all_ices_ship_names(normalize_ship_names=True),
        ).execute()

        survey = inquirer.fuzzy(
            message="Select Survey from " + ship_name,
            choices=get_all_survey_names_from_a_ship(ship_name),
        ).execute()

        echosounders = inquirer.select(
            message="Select Echosounder from " + survey,
            choices=get_all_echosounders_in_a_survey(ship_name, survey),
        ).execute()

        print(f"You selected echosounder: {echosounders}")


if __name__ == "__main__":
    main()
