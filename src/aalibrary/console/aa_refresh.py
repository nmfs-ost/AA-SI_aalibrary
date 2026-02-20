import subprocess
import sys


REPO_URL = "git+https://github.com/nmfs-ost/AA-SI_aalibrary.git@main"


def print_help() -> None:
    print(
        """\
Usage:
  aa-refresh [--help]

Description:
  Uninstalls the currently installed `aalibrary` package (if present) and
  reinstalls it directly from the AA-SI_aalibrary GitHub repository (main branch).

Notes:
  - Intended for use inside your active virtual environment.
  - Uses: python -m pip ... (so it targets the current interpreter).
"""
    )


def run(cmd: list[str]) -> int:
    """Run a command and return its exit code."""
    print("+", " ".join(cmd))
    return subprocess.run(cmd, check=False).returncode


def main() -> int:
    if "--help" in sys.argv or "-h" in sys.argv:
        print_help()
        return 0

    # Uninstall first. If it's not installed, pip may return non-zero; that's OK.
    uninstall_rc = run([sys.executable, "-m", "pip", "uninstall", "-y", "aalibrary"])
    if uninstall_rc != 0:
        print("Note: uninstall returned a non-zero exit code (package may not have been installed). Continuing...")

    # Reinstall from GitHub (main).
    install_rc = run([sys.executable, "-m", "pip", "install", REPO_URL])
    if install_rc != 0:
        print("ERROR: install failed.", file=sys.stderr)
        return install_rc

    print("Done: aalibrary refreshed from GitHub (main).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
