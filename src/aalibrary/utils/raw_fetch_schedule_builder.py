#!/usr/bin/env python3
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import calendar
import random
import re
from typing import Any

from aalibrary.utils import ncei_cache_utils
from InquirerPy import inquirer

try:
    import yaml  # type: ignore
except Exception:
    yaml = None


# ----------------------------
# Helpers
# ----------------------------

def days_in_month(year: int, month: int) -> int:
    return calendar.monthrange(year, month)[1]


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


def select_int(message: str, values: list[int], *, max_height: str = "70%") -> int:
    choices = [{"name": str(v), "value": v} for v in values]
    return int(select_value(message, choices, max_height=max_height))


# ----------------------------
# Manual datetime input (validated)
# ----------------------------

_DATETIME_RE = re.compile(
    r"^\s*(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2})(?::(\d{2}))?(?:Z)?\s*$"
)


def parse_user_datetime_utc(text: str) -> datetime:
    """
    Parse a user-entered datetime as UTC.

    Accepted formats (UTC):
      - YYYY-MM-DD HH:MM
      - YYYY-MM-DD HH:MM:SS
      - YYYY-MM-DDTHH:MM
      - YYYY-MM-DDTHH:MM:SS
    Optional trailing 'Z' is allowed.

    Returns a timezone-aware datetime in UTC.
    Raises ValueError if invalid.
    """
    m = _DATETIME_RE.match(text or "")
    if not m:
        raise ValueError(
            "Invalid datetime format. Use 'YYYY-MM-DD HH:MM' or 'YYYY-MM-DD HH:MM:SS' (UTC). "
            "Examples: 2026-03-03 14:05   or   2026-03-03 14:05:30"
        )

    year, month, day, hour, minute, second = m.groups()
    sec = int(second) if second is not None else 0

    # datetime() raises ValueError for invalid dates/times (e.g., Feb 30, 25:00)
    return datetime(
        int(year), int(month), int(day),
        int(hour), int(minute), sec,
        tzinfo=timezone.utc,
    )


def pick_datetime(label: str) -> datetime:
    """
    Manual datetime entry with validation.
    Input is interpreted as UTC.
    """
    def _validator(s: str) -> bool:
        parse_user_datetime_utc(s)  # raises ValueError if invalid
        return True

    raw = inquirer.text(
<<<<<<< HEAD
        message=f"⏱️   {label} (UTC) [YYYY-MM-DD HH:MM:SS]:",
=======
        message=f"⏱️   {label} (UTC) [YYYY-MM-DD HH:MM[:SS]]:",
>>>>>>> origin/main
        validate=_validator,
        invalid_message=(
            "Invalid datetime. Use 'YYYY-MM-DD HH:MM' or 'YYYY-MM-DD HH:MM:SS' (UTC). "
            "Example: 2026-03-03 14:05"
        ),
    ).execute()

    return parse_user_datetime_utc(str(raw))


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
# Fake NOAA-ish data (swap-outs)
# ----------------------------

def get_vessel_names() -> list[str]:
    return [
        "Reuben Lasker",
        "Falkor",
        "Henry B. Bigelow",
        "Oscar Dyson",
        "Nancy Foster",
        "Pisces",
        "Delaware II",
        "Bell M. Shimada",
        "Gordon Gunter",
        "Albatross IV",
    ]


def get_fake_surveys_for_vessel(vessel: str, n: int = 60) -> list[str]:
    rng = random.Random(hash(vessel) & 0xFFFFFFFF)
    years = [2021, 2022, 2023, 2024, 2025, 2026]

    words = vessel.split()
    prefix = "".join([w[0] for w in words[:2]]).upper()
    if len(prefix) < 2:
        prefix = (prefix + "X")[:2]

    surveys: list[str] = []
    for _ in range(n):
        yy = rng.choice(years) % 100
        leg = rng.randint(1, 9)
        seq = rng.randint(0, 9)
        surveys.append(f"{prefix}{yy:02d}{leg}{seq}")

    out: list[str] = []
    seen: set[str] = set()
    for s in surveys:
        if s not in seen:
            out.append(s)
            seen.add(s)
    return out


def get_instruments() -> list[str]:
    return ["EK60", "EK80", "ADCP", "EK500"]


# ----------------------------
# Data model
# ----------------------------

@dataclass
class TimeWindow:
    start: str
    end: str


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


def create_time_window() -> TimeWindow:
    """Prompt for Start + End, validate end > start."""
    while True:
        try:
            start_dt = pick_datetime("Start")
            end_dt = pick_datetime("End")
        except ValueError as e:
            # parse_user_datetime_utc throws ValueError with a good message
            print(f"\n🚫 {e}\n")
            continue

        if end_dt <= start_dt:
            print("\n🚫 End must be after Start. Please try again.\n")
            continue

        return TimeWindow(start=to_utc_z(start_dt), end=to_utc_z(end_dt))


def build_request() -> Request:
    vessel = choose_vessel()
    survey = choose_survey(vessel)
    instrument = choose_instrument(vessel, survey)

    windows: list[TimeWindow] = []
    while True:
        windows.append(create_time_window())

        more = inquirer.confirm(
            message="➕   Add another time window for this same vessel/survey/instrument?",
            default=False,
        ).execute()
        if not more:
            break

    return Request(vessel=vessel, survey=survey, instrument=instrument, time_windows=windows)


def write_yaml_file(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if yaml is None:
        import json
        path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    else:
        text = yaml.safe_dump(data, sort_keys=False)
        path.write_text(text, encoding="utf-8")


def main(output_path: Path | None = None) -> Path:
    """
    Runs the interactive builder and saves YAML.
    If output_path is provided, skips the output-path prompt.
    Returns the saved path.
    """
    print("\n🧭   aa-get : Fetch Schedule Builder\n")

    requests: list[Request] = []

    while True:
        requests.append(build_request())

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
                "time-windows": [{"start": w.start, "end": w.end} for w in r.time_windows],
            }
            for r in requests
        ]
    }

    out_path = output_path if output_path is not None else ask_output_path()
    out_path = Path(out_path).expanduser()
    if out_path.suffix == "":
        out_path = out_path.with_suffix(".yaml")

    write_yaml_file(out_path, schedule_dict)

    print("\n🧾   Result (YAML)\n")
    if yaml is None:
        import json
        print(json.dumps(schedule_dict, indent=2))
        print(f"\n💾   Saved (JSON fallback) to: {out_path}\n")
    else:
        print(yaml.safe_dump(schedule_dict, sort_keys=False))
        print(f"\n💾   Saved YAML to: {out_path}\n")

    print("✅   Done.\n")
    return out_path


if __name__ == "__main__":
    main()