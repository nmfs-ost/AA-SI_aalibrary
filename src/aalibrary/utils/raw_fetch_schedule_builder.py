#!/usr/bin/env python3
"""
raw_fetch_schedule_builder

Interactive InquirerPy UI for building an Active-Acoustics fetch schedule
YAML. Public surface (kept stable for aa-get):

    default_output_path() -> Path
    main(output_path: Path | None = None) -> Path | None

`main` returns the saved path on success, or None if the user declined to
save. The YAML schema is unchanged:

    requests:
      - vessel: "..."
        survey: "..."
        instrument: "..."
        time-windows:
          - start-date: "YYYY-MM-DD"
            start-time: "HH:MM:SS"
            end-date:   "YYYY-MM-DD"
            end-time:   "HH:MM:SS"
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone, date
from pathlib import Path
import re
from typing import Any, Iterable

from aalibrary.utils import ncei_cache_utils
from InquirerPy import inquirer

try:
    import yaml  # type: ignore
except Exception:
    yaml = None

# ----------------------------
# Optional rich integration
# ----------------------------
# rich is treated as a soft dependency: when present we use Panels, Rules,
# Tables and YAML syntax highlighting; when absent we fall back to plain
# emoji-prefixed prints so the UI still works on minimal environments.

try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.rule import Rule
    from rich.table import Table
    from rich.syntax import Syntax
    from rich.text import Text
    _RICH_AVAILABLE = True
except Exception:
    _RICH_AVAILABLE = False
    Console = Panel = Rule = Table = Syntax = Text = None  # type: ignore

# Module-level console handle. Created lazily inside main() so it captures
# whatever sys.stdout is at the time the UI runs (aa-get may have swapped
# stdout → stderr to keep the pipeline clean).
_console: "Console | None" = None


def _make_console() -> "Console | None":
    if not _RICH_AVAILABLE:
        return None
    return Console()


# ----------------------------
# Pretty-print helpers (rich-aware, with plain fallback)
# ----------------------------

def _print_banner() -> None:
    title = "aa-get  ·  Fetch Schedule Builder"
    subtitle = "Build a YAML schedule, then pipe it to aa-fetch."
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
        print("\n🧭   aa-get : Fetch Schedule Builder")
        print(f"     {subtitle}\n")


def _print_section(label: str) -> None:
    if _console is not None:
        _console.print()
        _console.print(Rule(f"[bold]{label}[/bold]", style="cyan"))
    else:
        print(f"\n── {label} " + "─" * max(0, 50 - len(label)))


def _print_info(text: str) -> None:
    if _console is not None:
        _console.print(f"[dim]{text}[/dim]")
    else:
        print(text)


def _print_warning(text: str) -> None:
    if _console is not None:
        _console.print(f"[yellow]⚠️   {text}[/yellow]")
    else:
        print(f"⚠️   {text}")


def _print_error(text: str) -> None:
    if _console is not None:
        _console.print(f"[red]🚫 {text}[/red]")
    else:
        print(f"🚫 {text}")


def _print_success(text: str) -> None:
    if _console is not None:
        _console.print(f"[green]✅   {text}[/green]")
    else:
        print(f"✅   {text}")


# ----------------------------
# Helpers (UTC, validation)
# ----------------------------

def to_utc_z(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def select_value(message: str, choices: list[dict[str, Any]], *, max_height: str = "70%") -> Any:
    return inquirer.select(
        message=message,
        choices=choices,
        max_height=max_height,
    ).execute()


# ----------------------------
# Manual time input (validated)
# ----------------------------

_TIME_RE = re.compile(r"^\s*(\d{2}):(\d{2})(?::(\d{2}))?\s*$")


def parse_user_time(text: str) -> tuple[int, int, int]:
    """
    Parse user-entered time in HH:MM or HH:MM:SS format.

    Returns (hour, minute, second). Raises ValueError on invalid input.
    """
    m = _TIME_RE.match(text or "")
    if not m:
        raise ValueError("Invalid time format. Use HH:MM or HH:MM:SS. Example: 14:05 or 14:05:30")

    hh_s, mm_s, ss_s = m.groups()
    hh = int(hh_s)
    mm = int(mm_s)
    ss = int(ss_s) if ss_s is not None else 0

    if not (0 <= hh <= 23):
        raise ValueError("Hour must be 00–23.")
    if not (0 <= mm <= 59):
        raise ValueError("Minute must be 00–59.")
    if not (0 <= ss <= 59):
        raise ValueError("Second must be 00–59.")

    return hh, mm, ss


def pick_time_text(label: str, *, default: str = "00:00:00") -> tuple[int, int, int]:
    """Manual HH:MM[:SS] entry with validation."""

    def _time_validator(s: str) -> bool:
        parse_user_time(s)
        return True

    raw_time = inquirer.text(
        message=f"⏱️   {label} time (UTC) [HH:MM[:SS]]:",
        default=default,
        validate=_time_validator,
        invalid_message="Invalid time. Use HH:MM or HH:MM:SS (UTC). Example: 14:05 or 14:05:30",
    ).execute()

    return parse_user_time(str(raw_time))


# ----------------------------
# Date selection based on available survey dates
# ----------------------------

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _normalize_available_dates(raw_dates: Iterable[Any]) -> list[str]:
    """
    Convert ncei_cache_utils.get_dates_of_survey_in_ncei_cache(...) output
    into a sorted list of unique YYYY-MM-DD strings.

    Defensive: raw_dates may be strings, datetime/date objects, or other
    representations depending on cache implementation.
    """
    out: set[str] = set()
    for d in raw_dates:
        if d is None:
            continue
        if isinstance(d, str):
            s = d.strip()
            if len(s) >= 10:
                s10 = s[:10]
                if _DATE_RE.match(s10):
                    out.add(s10)
        elif isinstance(d, datetime):
            out.add(d.astimezone(timezone.utc).strftime("%Y-%m-%d"))
        elif isinstance(d, date):
            out.add(d.strftime("%Y-%m-%d"))
        else:
            s = str(d).strip()
            if len(s) >= 10:
                s10 = s[:10]
                if _DATE_RE.match(s10):
                    out.add(s10)

    return sorted(out)


def get_available_dates_for_survey(vessel: str, survey: str) -> list[str]:
    """Pull available dates for (vessel, survey) from the NCEI cache utility."""
    raw_dates = ncei_cache_utils.get_dates_of_survey_in_ncei_cache(survey_name=survey)
    dates = _normalize_available_dates(raw_dates)
    if not dates:
        raise RuntimeError(
            f"No available dates found for vessel={vessel!r}, survey={survey!r} in NCEI cache."
        )
    return dates


def pick_date_from_available(label: str, available_dates: list[str]) -> str:
    """
    Pick a date (YYYY-MM-DD) from a list of available dates.

    Uses fuzzy search so typing e.g. "2012-08" filters quickly even when the
    survey has hundreds of days. Falls back to plain select for very small
    lists where fuzzy is overkill.
    """
    if len(available_dates) <= 12:
        return str(
            inquirer.select(
                message=f"📆   {label} date:",
                choices=available_dates,
                max_height="70%",
            ).execute()
        )

    return str(
        inquirer.fuzzy(
            message=f"📆   {label} date (type to filter):",
            choices=available_dates,
            max_height="70%",
        ).execute()
    )


def pick_datetime_parts(label: str, available_dates: list[str]) -> tuple[str, str, datetime]:
    """
    Collect (date_str, time_str, dt_utc):
      date_str = YYYY-MM-DD (must be in available_dates)
      time_str = HH:MM:SS   (manual validated)
      dt_utc   = timezone-aware datetime in UTC
    """
    date_str = pick_date_from_available(label, available_dates)
    hh, mm, ss = pick_time_text(label, default="00:00:00")
    y, m, d = int(date_str[0:4]), int(date_str[5:7]), int(date_str[8:10])
    dt = datetime(y, m, d, hh, mm, ss, tzinfo=timezone.utc)
    return date_str, f"{hh:02d}:{mm:02d}:{ss:02d}", dt


# ----------------------------
# Output path helpers
# ----------------------------

def default_output_path() -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return Path.cwd() / f"fetch_request_{stamp}.yaml"


def ask_output_path() -> Path:
    raw = inquirer.text(
        message="💾   Output YAML path:",
        default=str(default_output_path()),
    ).execute()

    path = Path(str(raw)).expanduser()
    if path.suffix == "":
        path = path.with_suffix(".yaml")
    return path


# ----------------------------
# Data model
# ----------------------------

@dataclass
class TimeWindow:
    start_date: str
    start_time: str
    end_date: str
    end_time: str


@dataclass
class Request:
    vessel: str
    survey: str
    instrument: str
    time_windows: list[TimeWindow]


# ----------------------------
# UI flows
# ----------------------------

def choose_vessel() -> str:
    vessels = ncei_cache_utils.get_all_ship_names_in_ncei_cache()
    return str(
        inquirer.fuzzy(
            message="🛥️   Select vessel (type to search):",
            choices=vessels,
            max_height="70%",
        ).execute()
    )


def choose_survey(vessel: str) -> str:
    surveys = ncei_cache_utils.get_all_survey_names_from_a_ship_in_ncei_cache(vessel)
    return str(
        inquirer.fuzzy(
            message=f"📋   Select survey for {vessel} (type to search):",
            choices=surveys,
            max_height="70%",
        ).execute()
    )


def choose_instrument(ship_name: str, survey_name: str) -> str:
    instruments = ncei_cache_utils.get_all_echosounders_in_a_survey_in_ncei_cache(
        ship_name=ship_name,
        survey_name=survey_name,
        gcp_bq_client=None,
        return_full_paths=None,
    )
    return str(
        inquirer.select(
            message="🎛️   Select instrument:",
            choices=instruments,
            max_height="70%",
        ).execute()
    )


def create_time_window(*, available_dates: list[str], window_idx: int) -> TimeWindow:
    """
    Prompt for Start (date from available list + time via text) and End (same),
    validate end > start (UTC).
    """
    _print_section(f"Time window {window_idx}")

    # Show a quick orientation: how many dates, and the range covered.
    _print_info(
        f"{len(available_dates)} available date(s): "
        f"{available_dates[0]} → {available_dates[-1]}"
    )

    while True:
        try:
            s_date, s_time, s_dt = pick_datetime_parts("Start", available_dates)
            e_date, e_time, e_dt = pick_datetime_parts("End", available_dates)
        except ValueError as e:
            _print_error(str(e))
            continue

        if e_dt <= s_dt:
            _print_error("End must be strictly after Start. Please try again.")
            continue

        return TimeWindow(
            start_date=s_date,
            start_time=s_time,
            end_date=e_date,
            end_time=e_time,
        )


def build_request(request_idx: int) -> Request:
    _print_section(f"Request {request_idx} · Vessel")
    vessel = choose_vessel()

    _print_section(f"Request {request_idx} · Survey")
    survey = choose_survey(vessel)

    _print_section(f"Request {request_idx} · Instrument")
    instrument = choose_instrument(vessel, survey)

    # Constrain date picking to dates actually present in the cache.
    available_dates = get_available_dates_for_survey(vessel, survey)

    windows: list[TimeWindow] = []
    window_idx = 1
    while True:
        windows.append(
            create_time_window(available_dates=available_dates, window_idx=window_idx)
        )
        window_idx += 1

        more = inquirer.confirm(
            message="➕   Add another time window for this same vessel/survey/instrument?",
            default=False,
        ).execute()
        if not more:
            break

    return Request(vessel=vessel, survey=survey, instrument=instrument, time_windows=windows)


# ----------------------------
# Summary / preview
# ----------------------------

def _print_summary(requests: list[Request]) -> None:
    """Show a compact table of every request before saving."""
    _print_section("Schedule summary")

    if _console is not None:
        table = Table(show_header=True, header_style="bold cyan", border_style="cyan")
        table.add_column("#", justify="right", style="dim", width=3)
        table.add_column("Vessel", style="bold")
        table.add_column("Survey")
        table.add_column("Instrument")
        table.add_column("Time windows")

        for i, r in enumerate(requests, start=1):
            windows_repr = "\n".join(
                f"{w.start_date} {w.start_time} → {w.end_date} {w.end_time}"
                for w in r.time_windows
            )
            table.add_row(str(i), r.vessel, r.survey, r.instrument, windows_repr)

        _console.print(table)
    else:
        for i, r in enumerate(requests, start=1):
            print(f"  [{i}] {r.vessel} / {r.survey} / {r.instrument}")
            for w in r.time_windows:
                print(f"        {w.start_date} {w.start_time} → {w.end_date} {w.end_time}")


def _print_yaml_preview(schedule_dict: dict[str, Any]) -> None:
    """Pretty-print the YAML that will be / was saved."""
    if yaml is None:
        # JSON fallback path
        import json
        text = json.dumps(schedule_dict, indent=2)
        if _console is not None:
            _console.print(Panel(text, title="JSON (yaml not installed)", border_style="yellow"))
        else:
            print("\n🧾   Result (JSON fallback)\n")
            print(text)
        return

    text = yaml.safe_dump(schedule_dict, sort_keys=False)

    if _console is not None:
        _console.print()
        _console.print(
            Panel(
                Syntax(text, "yaml", theme="ansi_dark", background_color="default"),
                title="🧾  Resulting YAML",
                border_style="green",
            )
        )
    else:
        print("\n🧾   Result (YAML)\n")
        print(text)


# ----------------------------
# YAML write
# ----------------------------

def write_yaml_file(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if yaml is None:
        import json
        path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        return

    # Force quotes on all scalars so date/time-like values are always strings
    class _QuotedDumper(yaml.SafeDumper):
        pass

    def _quoted_str_representer(dumper, value: str):
        return dumper.represent_scalar("tag:yaml.org,2002:str", value, style='"')

    _QuotedDumper.add_representer(str, _quoted_str_representer)

    text = yaml.dump(
        data,
        Dumper=_QuotedDumper,
        sort_keys=False,
        default_flow_style=False,
        allow_unicode=True,
    )
    path.write_text(text, encoding="utf-8")


# ----------------------------
# Top-level entry point
# ----------------------------

def main(output_path: Path | None = None) -> Path | None:
    """
    Run the interactive builder and save YAML.

    Returns the saved path on success, or None if the user declined to save.
    Raises KeyboardInterrupt to the caller if the user hits Ctrl+C — aa-get
    handles that as a clean cancel.
    """
    global _console
    _console = _make_console()

    _print_banner()

    requests: list[Request] = []
    request_idx = 1

    while True:
        try:
            requests.append(build_request(request_idx))
        except RuntimeError as e:
            # e.g. "No available dates found for vessel=..., survey=..."
            _print_error(str(e))
            retry = inquirer.confirm(
                message="🔁   Try a different vessel/survey?",
                default=True,
            ).execute()
            if not retry:
                return None
            continue

        request_idx += 1

        another_req = inquirer.confirm(
            message="🧩   Create another request (different vessel/survey/instrument)?",
            default=False,
        ).execute()
        if not another_req:
            break

    schedule_dict: dict[str, Any] = {
        "requests": [
            {
                "vessel": r.vessel,
                "survey": r.survey,
                "instrument": r.instrument,
                "time-windows": [
                    {
                        "start-date": w.start_date,
                        "start-time": w.start_time,
                        "end-date": w.end_date,
                        "end-time": w.end_time,
                    }
                    for w in r.time_windows
                ],
            }
            for r in requests
        ]
    }

    # Show what we're about to save and let the user back out.
    _print_summary(requests)
    _print_yaml_preview(schedule_dict)

    save_confirmed = inquirer.confirm(
        message="💾   Save this schedule?",
        default=True,
    ).execute()
    if not save_confirmed:
        _print_warning("Save cancelled by user. Nothing was written.")
        return None

    # Resolve output path (use the one aa-get passed in, or ask).
    out_path = output_path if output_path is not None else ask_output_path()
    out_path = Path(out_path).expanduser()
    if out_path.suffix == "":
        out_path = out_path.with_suffix(".yaml")

    # Overwrite confirmation if the target already exists.
    if out_path.exists():
        overwrite = inquirer.confirm(
            message=f"⚠️   '{out_path}' already exists. Overwrite?",
            default=False,
        ).execute()
        if not overwrite:
            _print_warning("Overwrite declined. Nothing was written.")
            return None

    write_yaml_file(out_path, schedule_dict)
    _print_success(f"Saved YAML to: {out_path}")

    return out_path


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        # Friendly message instead of a stack trace.
        if _console is not None:
            _console.print("\n[yellow]Cancelled.[/yellow]")
        else:
            print("\nCancelled.")
        raise SystemExit(130)