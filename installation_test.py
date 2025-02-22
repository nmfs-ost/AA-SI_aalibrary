"""This script is used to test installations of the aalibrary on various
systems and platforms."""

import sys
import platform
import subprocess


def determine_os():
    return platform.system()


def check_and_install_on_windows():
    system_name = determine_os()

    assert system_name == "Windows", (
        "The system specified was `Windows` however, we have determined that"
        f" the system is `{system_name}`"
    )

    # Install the library using the current directory.
    subprocess.check_call(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "aalibrary@git+https://github.com/nmfs-ost/AA-SI_aalibrary.git",
        ]
    )


def check_and_install_on_linux():
    system_name = determine_os()

    assert system_name == "Linux", (
        "The system specified was `Linux` however, we have determined that "
        f"the system is `{system_name}`"
    )

    # Install the library using the current directory.
    subprocess.check_call(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "aalibrary@git+https://github.com/nmfs-ost/AA-SI_aalibrary.git",
        ]
    )


def check_and_install_on_mac():
    system_name = determine_os()

    assert system_name == "Darwin", (
        "The system specified was `Darwin` however, we have determined that "
        f"the system is `{system_name}`"
    )

    # Install the library using the current directory.
    subprocess.check_call(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "aalibrary@git+https://github.com/nmfs-ost/AA-SI_aalibrary.git",
        ]
    )


def install(system_name: str = ""):
    """Installs the package on the current system after determining the
    current system.

    Args:
        system_name (str, optional): The name of the system you are using. Can
            be one of ['Windows', 'Linux', 'Darwin' (for MacOS)]. Will
            determine automatically if not specified. Defaults to "".
    """

    if system_name == "":
        system_name = determine_os()

    if system_name == "Windows":
        check_and_install_on_windows()
    elif system_name == "Linux":
        check_and_install_on_linux()
    elif system_name == "Darwin":  # Covers MacOS
        check_and_install_on_mac()


if __name__ == "__main__":
    install()
