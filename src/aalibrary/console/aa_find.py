#!/usr/bin/env python3
"""
aa-find

Interactive console application to search and browse acoustics data on
NCEI (and, eventually, OMAO). Drills down vessel → survey → sonar model →
raw file → operation, with hooks into the rest of the aa-pipeline
(`aa-raw`, `aa-plot`).

This is a TUI — it does NOT participate in the aa-pipeline `|` chain.
It calls those tools as subprocesses on your behalf when you pick an
operation that needs them.
"""

# === Silence noisy library logs BEFORE any heavy imports ===
import logging
import sys
import warnings

logging.disable(logging.CRITICAL)
warnings.filterwarnings("ignore")

from loguru import logger
logger.remove()
logger.add(sys.stderr, level="WARNING")

# Heavy imports
import os
import subprocess
from contextlib import contextmanager

from InquirerPy import inquirer

from aalibrary.utils.cloud_utils import create_s3_objs
from aalibrary.utils.ncei_utils import (
    get_all_survey_names_from_a_ship,
    get_all_echosounders_in_a_survey,
    get_all_ship_names_in_ncei,
    get_all_raw_file_names_from_survey,
    get_folder_size_from_s3,
    get_file_size_from_s3,
)

# ----------------------------
# Optional rich integration
# ----------------------------
try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.rule import Rule
    from rich.table import Table
    from rich.text import Text
    _RICH = True
except Exception:
    _RICH = False
    Console = Panel = Rule = Table = Text = None  # type: ignore

_console: "Console | None" = None  # set lazily in main()


def _make_console() -> "Console | None":
    return Console() if _RICH else None


# ----------------------------
# Pretty-print helpers
# ----------------------------

BACK = "← Back"


def _clear() -> None:
    os.system("cls" if os.name == "nt" else "clear")


def _print_banner() -> None:
    title = "🔭  aa-find"
    subtitle = "Browse NCEI / OMAO vessel acoustics data interactively."
    if _console is not None:
        _console.print()
        _console.print(
            Panel.fit(
                Text.assemble(
                    (title, "bold cyan"),
                    "\n",
                    (subtitle, "dim"),
                ),
                border_style="cyan",
                padding=(0, 2),
            )
        )
        _console.print()
    else:
        print(f"\n  {title}")
        print(f"  {subtitle}\n")


def _breadcrumb(*parts: str) -> None:
    """Show 'Ship › Survey › Sonar' as a colored rule."""
    text = " › ".join(parts)
    if _console is not None:
        _console.print()
        _console.print(Rule(f"[bold]{text}[/bold]", style="cyan"))
    else:
        print(f"\n── {text} ──")


def _info(msg: str) -> None:
    if _console is not None:
        _console.print(f"[dim]{msg}[/dim]")
    else:
        print(msg)


def _success(msg: str) -> None:
    if _console is not None:
        _console.print(f"[green]✓[/green] {msg}")
    else:
        print(f"✓ {msg}")


def _warn(msg: str) -> None:
    if _console is not None:
        _console.print(f"[yellow]⚠[/yellow] {msg}")
    else:
        print(f"⚠ {msg}")


def _error(msg: str) -> None:
    if _console is not None:
        _console.print(f"[red]✗[/red] {msg}")
    else:
        print(f"✗ {msg}")


def _not_available(feature: str) -> None:
    msg = f"{feature} is not yet available."
    if _console is not None:
        _console.print(
            Panel(
                f"[yellow]{msg}[/yellow]\n[dim]Coming in a future release.[/dim]",
                title="Not implemented",
                border_style="yellow",
                padding=(0, 2),
            )
        )
    else:
        print(f"\n⚠ {msg}\n")


def _press_enter(prompt: str = "Press Enter to continue") -> None:
    if _console is not None:
        _console.input(f"[dim]{prompt}[/dim] ")
    else:
        input(f"{prompt} ")


@contextmanager
def _status(message: str):
    """Spinner during long operations. Falls back to a plain print."""
    if _console is not None:
        with _console.status(f"[cyan]{message}[/cyan]", spinner="dots"):
            yield
    else:
        print(message)
        yield


def _format_bytes(b) -> str:
    """Human-readable byte sizes. The original printed
       '257103872 b|245.32 MB|0.24 GB' which was hard to read."""
    if b is None:
        return "unknown"
    n = float(b)
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    for u in units:
        if n < 1024 or u == units[-1]:
            return f"{int(n)} {u}" if u == "B" else f"{n:,.2f} {u}"
        n /= 1024
    return f"{n:,.2f} EB"


# ----------------------------
# Selection helpers
# ----------------------------

def _pick(message: str, choices, *, fuzzy: bool = False, default=None):
    """Wrap inquirer.fuzzy / inquirer.select with consistent kwargs."""
    fn = inquirer.fuzzy if fuzzy else inquirer.select
    kwargs = dict(message=message, choices=choices, max_height="70%")
    if default is not None:
        kwargs["default"] = default
    return fn(**kwargs).execute()


# ----------------------------
# Resources / Documentation
# ----------------------------

# Preserved verbatim from the original list (one entry without a URL).
_RESOURCES = [
    ("AA-SI Homepage", None),
    ("NCEI", "https://www.ncei.noaa.gov/"),
    ("OMAO", "https://www.omao.noaa.gov/"),
    ("OST", "https://www.fisheries.noaa.gov/about/office-science-and-technology"),
    ("AA-SI GitHub", "https://github.com/orgs/nmfs-ost/repositories?q=AA"),
    ("AA-SI_aalibrary", "https://github.com/nmfs-ost/AA-SI_aalibrary"),
    ("AA-SI_GCPSetup", "https://github.com/nmfs-ost/AA-SI_GCPSetup"),
    ("AA-SI_DataRoadMap", "https://github.com/nmfs-ost/AA-SI_DataRoadMap"),
    ("AA-SI_KMeans", "https://github.com/nmfs-ost/AA-SI_KMeans"),
    ("AA-SI_DBScan", "https://github.com/nmfs-ost/AA-SI_DBScan"),
]


def _show_resources() -> None:
    if _console is not None:
        table = Table(
            title="Resources & Documentation",
            border_style="cyan",
            show_header=True,
            header_style="bold cyan",
            title_style="bold",
        )
        table.add_column("Resource", style="bold", no_wrap=True)
        table.add_column("Link")
        for name, url in _RESOURCES:
            link_cell = f"[link={url}]{url}[/link]" if url else "—"
            table.add_row(name, link_cell)
        _console.print()
        _console.print(table)
        _console.print()
    else:
        print("\nResources & Documentation:\n")
        for name, url in _RESOURCES:
            print(f"  {name:<22} {url or '—'}")
        print()
    _press_enter("Press Enter to return to the main menu")


# ----------------------------
# Main menu
# ----------------------------

_MAIN_CHOICES = [
    {"name": "Search NCEI Vessel Data",        "value": "ncei"},
    {"name": "Search OMAO Vessel Data",        "value": "omao"},
    {"name": "Authenticate with Google",       "value": "google"},
    {"name": "View Resources & Documentation", "value": "docs"},
    {"name": "Exit Application",               "value": "exit"},
]


def main() -> None:
    """Top-level loop."""
    global _console
    _console = _make_console()

    _clear()
    _print_banner()

    try:
        while True:
            mode = _pick("Select an action:", _MAIN_CHOICES, default="ncei")

            if mode == "ncei":
                _handle_ncei_search()
            elif mode == "omao":
                _not_available("OMAO vessel search")
            elif mode == "google":
                _handle_google_auth()
            elif mode == "docs":
                _show_resources()
            elif mode == "exit":
                _farewell()
                return
    except KeyboardInterrupt:
        # Friendly exit, no traceback.
        _farewell()


def _farewell() -> None:
    _clear()
    if _console is not None:
        _console.print(
            Panel.fit(
                "[dim]Until next time.[/dim]",
                border_style="dim",
                padding=(0, 2),
            )
        )
        _console.print()
    else:
        print("\nUntil next time.\n")


# ----------------------------
# NCEI: vessel → survey → sonar → file → operation
# ----------------------------

def _handle_ncei_search() -> None:
    try:
        with _status("Fetching NCEI vessel list..."):
            s3_client, s3_resource, _ = create_s3_objs()
            ships = sorted(get_all_ship_names_in_ncei(s3_client=s3_client))
    except Exception as e:
        _error(f"Could not load vessel list: {e}")
        _press_enter()
        return

    if not ships:
        _warn("No vessels found.")
        _press_enter()
        return

    while True:
        ship = _pick("Select NCEI vessel:", [BACK] + ships, fuzzy=True)
        if ship == BACK:
            return
        _handle_ship(ship, s3_client, s3_resource)


def _handle_ship(ship: str, s3_client, s3_resource) -> None:
    _breadcrumb(ship)

    try:
        with _status(f"Fetching surveys for {ship}..."):
            surveys = sorted(
                get_all_survey_names_from_a_ship(ship_name=ship, s3_client=s3_client)
            )
    except Exception as e:
        _error(f"Could not load surveys for {ship}: {e}")
        _press_enter()
        return

    if not surveys:
        _warn(f"No surveys found for {ship}.")
        _press_enter()
        return

    while True:
        survey = _pick(
            f"Select survey from {ship}:",
            [BACK] + surveys,
            fuzzy=True,
        )
        if survey == BACK:
            return
        _handle_survey(ship, survey, s3_client, s3_resource)


def _handle_survey(ship: str, survey: str, s3_client, s3_resource) -> None:
    _breadcrumb(ship, survey)

    try:
        with _status(f"Fetching sonar models for {survey}..."):
            sonars = sorted(
                get_all_echosounders_in_a_survey(
                    ship_name=ship,
                    survey_name=survey,
                    s3_client=s3_client,
                )
            )
    except Exception as e:
        _error(f"Could not load sonar models: {e}")
        _press_enter()
        return

    if not sonars:
        _warn(f"No sonar models found for {survey}.")
        _press_enter()
        return

    while True:
        sonar = _pick(f"Select sonar model from {survey}:", [BACK] + sonars)
        if sonar == BACK:
            return
        _handle_sonar(ship, survey, sonar, s3_resource)


def _handle_sonar(ship: str, survey: str, sonar: str, s3_resource) -> None:
    _breadcrumb(ship, survey, sonar)

    try:
        with _status(f"Fetching .raw files for {sonar}..."):
            files = sorted(
                get_all_raw_file_names_from_survey(
                    ship_name=ship,
                    survey_name=survey,
                    echosounder=sonar,
                    s3_resource=s3_resource,
                )
            )
    except Exception as e:
        _error(f"Could not load raw files: {e}")
        _press_enter()
        return

    SURVEY_DISK = "[Survey Disk Usage]"

    while True:
        choice = _pick(
            f"Select a .raw file from {survey}:",
            [BACK, SURVEY_DISK] + files,
            fuzzy=True,
        )

        if choice == BACK:
            return

        if choice == SURVEY_DISK:
            _do_survey_disk_usage(ship, survey, sonar, s3_resource)
            continue

        _handle_file(ship, survey, sonar, choice, s3_resource)


def _handle_file(
    ship: str, survey: str, sonar: str, file_name: str, s3_resource
) -> None:
    _breadcrumb(ship, survey, sonar, file_name)

    operations = [
        {"name": BACK,                  "value": "back"},
        {"name": "Download .raw",       "value": "download_raw"},
        {"name": "Download .nc",        "value": "download_nc"},
        {"name": "Plot Echogram(s)",    "value": "plot"},
        {"name": "Run KMeans",          "value": "kmeans"},
        {"name": "Run DBScan",          "value": "dbscan"},
        {"name": "Check File Disk Usage", "value": "size"},
    ]

    while True:
        op = _pick(f"Action for {file_name}:", operations)

        if op == "back":
            return
        elif op == "download_raw":
            _do_download_raw(ship, survey, sonar, file_name)
        elif op == "download_nc":
            # The original menu listed this option but had no dispatch
            # handler for it — choosing it silently did nothing.
            _not_available("NetCDF download")
        elif op == "plot":
            # The original dispatched on the string 'Plot Echograms', but
            # the menu showed 'Plot Echogram(s)' with parens — so the
            # plot branch never fired. Now both come from the same
            # 'plot' value, so it can't drift.
            _do_plot(ship, survey, sonar, file_name)
        elif op == "kmeans":
            _not_available("KMeans clustering")
        elif op == "dbscan":
            _not_available("DBScan clustering")
        elif op == "size":
            _do_file_disk_usage(ship, survey, sonar, file_name, s3_resource)


# ----------------------------
# Operations
# ----------------------------

def _do_download_raw(ship: str, survey: str, sonar: str, file_name: str) -> None:
    folder = f"{ship}_{survey}_{sonar}_NCEI"
    os.makedirs(folder, exist_ok=True)
    _info(f"Downloading {file_name} → ./{folder}/")
    cmd = [
        "aa-raw",
        "--file_name", file_name,
        "--file_type", "raw",
        "--ship_name", ship,
        "--survey_name", survey,
        "--sonar_model", sonar,
        "--file_download_directory", folder,
    ]
    _run_subprocess(cmd, friendly_name="aa-raw")
    if os.path.isdir(folder):
        _success(f"Saved into ./{folder}/")


def _do_plot(ship: str, survey: str, sonar: str, file_name: str) -> None:
    folder = f"{ship}_{survey}_{sonar}"
    os.makedirs(folder, exist_ok=True)
    out = f"{folder}/echogram.png"
    _info(f"Plotting {file_name} → ./{out}")
    cmd = [
        "aa-plot",
        file_name,
        "--sonar_model", sonar,
        "--output-file", out,
    ]
    _run_subprocess(cmd, friendly_name="aa-plot")


def _do_survey_disk_usage(
    ship: str, survey: str, sonar: str, s3_resource
) -> None:
    try:
        with _status("Calculating survey disk usage..."):
            size = get_folder_size_from_s3(
                folder_prefix=f"data/raw/{ship}/{survey}/{sonar}/",
                s3_resource=s3_resource,
            )
        _info(f"Total survey size: {_format_bytes(size)}")
    except Exception as e:
        _error(f"Could not get survey size: {e}")


def _do_file_disk_usage(
    ship: str, survey: str, sonar: str, file_name: str, s3_resource
) -> None:
    try:
        with _status(f"Fetching size of {file_name}..."):
            size = get_file_size_from_s3(
                object_key=f"data/raw/{ship}/{survey}/{sonar}/{file_name}",
                s3_resource=s3_resource,
            )
        _info(f"File size: {_format_bytes(size)}")
    except Exception as e:
        _error(f"Could not get file size: {e}")


def _run_subprocess(cmd: list[str], *, friendly_name: str) -> None:
    """Run a child command and surface common failures as errors instead of
    bubbling stack traces / killing the TUI."""
    try:
        result = subprocess.run(cmd, check=False)
    except FileNotFoundError:
        _error(
            f"'{friendly_name}' command not found on PATH. "
            "Is the aa-pipeline installed in the active environment?"
        )
        return
    except KeyboardInterrupt:
        _warn(f"{friendly_name} interrupted.")
        return
    except Exception as e:
        _error(f"{friendly_name} failed: {e}")
        return

    if result.returncode != 0:
        _error(f"{friendly_name} exited with code {result.returncode}.")


# ----------------------------
# Google authentication
# ----------------------------

def _handle_google_auth() -> None:
    _info("Authenticating with Google Cloud...")

    # The original ran:
    #     gcloud config set account {ACCOUNT}
    # with `{ACCOUNT}` as a literal placeholder that was never substituted.
    # Combined with `check=True`, that step would either fail (best case)
    # or set the active account to the literal string "{ACCOUNT}". The
    # preceding `gcloud auth login` already activates the chosen account,
    # so the line was both broken and redundant — removed.
    commands = [
        ["gcloud", "auth", "login"],
        ["gcloud", "auth", "application-default", "login"],
        ["gcloud", "config", "set", "project", "ggn-nmfs-aa-dev-1"],
    ]

    for cmd in commands:
        _info("$ " + " ".join(cmd))
        try:
            subprocess.run(cmd, check=True)
        except FileNotFoundError:
            _error(
                "'gcloud' command not found on PATH. "
                "Install the Google Cloud SDK and try again."
            )
            return
        except subprocess.CalledProcessError as e:
            _error(
                f"Step '{' '.join(cmd)}' failed (exit {e.returncode}). "
                "You may need to retry; remaining steps were skipped."
            )
            return
        except KeyboardInterrupt:
            _warn("Authentication interrupted.")
            return

    _success("Google Cloud authentication complete.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        # Catch-all in case something interrupts before main()'s own handler
        # is in scope (e.g. during _make_console / banner).
        print()