#!/usr/bin/env python3
"""
aa-setup

(Re)install the AA-SI workstation environment on a Google Cloud VM: fetch
and run the AA-SI_GPCSetup init.sh, activate ~/venv313, log in for
Application Default Credentials, and set the gcloud project.

    aa-setup                          run the setup
    aa-setup --account you@noaa.gov   also select that gcloud account
    aa-setup --help                   curated help
"""

from __future__ import annotations

import argparse
import shlex
import subprocess
import sys

from aalibrary.console._core import Help, ToolSpec, render, show_help

INIT_URL = "https://raw.githubusercontent.com/nmfs-ost/AA-SI_GPCSetup/main/init.sh"
PROJECT = "ggn-nmfs-aa-dev-1"

SPEC = ToolSpec(name="aa-setup", role="utility", engines=())

HELP = Help(
    summary="Reinstall the AA-SI workstation environment on a Google Cloud VM.",
    does=(
        "Downloads the current init.sh from the AA-SI_GPCSetup repository into "
        "your home directory (replacing any old copy), runs it, activates "
        "~/venv313, runs 'gcloud auth application-default login' (opens a "
        "browser sign-in; this is the credential every aa-* tool uses for "
        "gs:// and BigQuery), and sets the gcloud project to "
        f"{PROJECT}. With --account it also selects that gcloud account. "
        "Each step runs only if the one before it succeeded."
    ),
    stdin="Nothing.",
    stdout="The setup script's own output, and the gcloud prompts.",
    options=[
        ("--account EMAIL", "also run 'gcloud config set account EMAIL' (default: "
                            "leave the active account as it is)"),
    ],
    files=(
        "Writes ~/init.sh, downloaded (with sudo) from\n\n"
        f"  {INIT_URL}\n\n"
        "What init.sh installs is defined in that repository. Needs network access."
    ),
    pipeline="Not a pipeline stage. Run it on its own, in a terminal.",
    examples=[
        "aa-setup",
        "aa-setup --account first.last@noaa.gov",
    ],
    notes=[
        "Exits with the status of the first step that failed (0 when all succeed). "
        "After it finishes, open a new shell or 'source ~/venv313/bin/activate' "
        "to use the environment it set up.",
    ],
)


def print_help():
    """Curated help (also used by the docs generator)."""
    sys.stdout.write(render(SPEC, HELP, _build_parser()))


def print_help_full():
    help_text = f"""
    Usage: aa-setup [--account EMAIL]

    Description:
    Reinstalls the startup script for the AA-SI GPCSetup environment on a
    Google Cloud VM, then logs in for Application Default Credentials and
    sets the gcloud project. Runs, in order, stopping at the first failure:

        cd ~
        sudo rm -f init.sh
        sudo wget {INIT_URL}
        sudo chmod +x init.sh
        ./init.sh
        source ~/venv313/bin/activate
        gcloud auth application-default login
        gcloud config set account EMAIL        (only with --account EMAIL)
        gcloud config set project {PROJECT}

    Options:
    --account EMAIL     gcloud account to select. Default: keep the active one.
    -h, --help          Curated help.
    --help-all          This text.
    """
    print(help_text)


def _build_parser():
    p = argparse.ArgumentParser(prog="aa-setup", add_help=False)
    p.add_argument("--account", default=None, metavar="EMAIL",
                   help="gcloud account to select (gcloud config set account).")
    return p


def build_command(account: str | None) -> str:
    """The bash script aa-setup runs."""
    steps = [
        "cd ~",
        "sudo rm -f init.sh",
        f"sudo wget {shlex.quote(INIT_URL)}",
        "sudo chmod +x init.sh",
        "./init.sh",
        "cd ~",
        "source venv313/bin/activate",
        "gcloud auth application-default login",
    ]
    if account:
        steps.append(f"gcloud config set account {shlex.quote(account)}")
    steps.append(f"gcloud config set project {PROJECT}")
    return " && \\\n".join(steps)


def main():
    parser = _build_parser()
    if show_help(SPEC, HELP, parser, full=print_help_full):
        sys.exit(0)
    args = parser.parse_args()

    # Run the full shell command
    result = subprocess.run(build_command(args.account), shell=True, executable="/bin/bash")
    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
