from aalibrary.ices_ship_names import get_all_ices_ship_names
from InquirerPy import inquirer

def main():
    choice = inquirer.fuzzy(
        message="Select a white ship:",
        choices=get_all_ices_ship_names(normalize_ship_names=True),
    ).execute()

    print(f"You selected: {choice}")

if __name__ == "__main__":
    main()