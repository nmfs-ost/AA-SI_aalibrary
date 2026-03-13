import subprocess
import sys


LIBRARIES = [
    {
        "pip_name": "aalibrary",
        "repo_url": "git+https://github.com/nmfs-ost/AA-SI_aalibrary.git@main",
    },
    {
        "pip_name": "AA-SI-KMEANS",
        "repo_url": "git+https://github.com/nmfs-ost/AA-SI_KMeans.git@main",
    },
]


def print_help() -> None:
    print(
        """\
Usage:
  aa-refresh [--help] [--only <pip_name>]

Description:
  Uninstalls and reinstalls development libraries from their GitHub
  repositories (main branch).  Uses --no-cache-dir and --force-reinstall
  so setuptools re-discovers any new sub-packages on install.

  Libraries refreshed:
    aalibrary        (AA-SI_aalibrary)
    AA-SI-KMEANS     (AA-SI_KMeans)

Options:
  --only <pip_name>   Refresh a single library instead of all.
                      e.g.  aa-refresh --only AA-SI-KMEANS
  --help, -h          Show this help message.

Notes:
  - Intended for use inside your active virtual environment.
  - Uses: python -m pip ... (so it targets the current interpreter).
"""
    )


def run(cmd: list[str]) -> int:
    """Run a command and return its exit code."""
    print("+", " ".join(cmd))
    return subprocess.run(cmd, check=False).returncode


def refresh(pip_name: str, repo_url: str) -> int:
    """Uninstall, clear pip cache for the package, and reinstall from GitHub."""

    print(f"\n{'=' * 60}")
    print(f"  Refreshing: {pip_name}")
    print(f"{'=' * 60}")

    # 1. Uninstall (non-zero is fine if it wasn't installed)
    uninstall_rc = run([sys.executable, "-m", "pip", "uninstall", "-y", pip_name])
    if uninstall_rc != 0:
        print(f"  Note: uninstall of {pip_name} returned non-zero "
              f"(may not have been installed). Continuing...")

    # 2. Reinstall from GitHub with --no-cache-dir and --force-reinstall
    #    --no-cache-dir  → ignores any cached wheel that baked in the old package list
    #    --force-reinstall → guarantees a fresh build even if pip thinks it's satisfied
    install_rc = run([
        sys.executable, "-m", "pip", "install",
        "--no-cache-dir",
        "--force-reinstall",
        repo_url,
    ])

    if install_rc != 0:
        print(f"  ERROR: install of {pip_name} failed.", file=sys.stderr)
        return install_rc

    print(f"  Done: {pip_name} refreshed from GitHub (main).")
    return 0


def main() -> int:
    if "--help" in sys.argv or "-h" in sys.argv:
        print_help()
        return 0

    # --only <pip_name> filter
    targets = LIBRARIES
    if "--only" in sys.argv:
        idx = sys.argv.index("--only")
        if idx + 1 >= len(sys.argv):
            print("ERROR: --only requires a pip_name argument.", file=sys.stderr)
            return 1
        only = sys.argv[idx + 1]
        targets = [lib for lib in LIBRARIES if lib["pip_name"] == only]
        if not targets:
            known = ", ".join(lib["pip_name"] for lib in LIBRARIES)
            print(f"ERROR: unknown library '{only}'. Known: {known}", file=sys.stderr)
            return 1

    failed = []
    for lib in targets:
        rc = refresh(lib["pip_name"], lib["repo_url"])
        if rc != 0:
            failed.append(lib["pip_name"])

    if failed:
        print(f"\nFailed to refresh: {', '.join(failed)}", file=sys.stderr)
        return 1

    print(f"\nAll libraries refreshed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())