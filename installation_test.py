"""This script is used to test installations of the aalibrary on various systems."""

import sys, os, platform


def determine_os():
    return platform.system()


def check_and_install_on_windows(): ...


def check_and_install_on_linux(): ...


def check_and_install_on_mac(): ...


def install(): ...


if __name__ == "__main__":
    determine_os()
