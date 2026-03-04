#!/usr/bin/env python3
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
# Helpers
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
    Parse user-entered time.

    Accepted formats:
      HH:MM
      HH:MM:SS

    Returns (hour, minute, second). Raises ValueError if invalid.
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
    """
    Time picker:
      Manual text entry HH:MM[:SS] with validation.
    """
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
    Convert the output of ncei_cache_utils.get_dates_of_survey_in_ncei_cache(...)
    into a sorted list of unique YYYY-MM-DD strings.

    This is defensive: raw_dates might already be strings, or datetime/date objects,
    or other representations depending on cache implementation.
    """
    out: set[str] = set()
    for d in raw_dates:
        if d is None:
            continue
        if isinstance(d, str):
            s = d.strip()
            # accept "YYYY-MM-DD" or full ISO like "YYYY-MM-DDTHH:MM:SSZ"
            if len(s) >= 10:
                s10 = s[:10]
                if _DATE_RE.match(s10):
                    out.add(s10)
        elif isinstance(d, datetime):
            out.add(d.astimezone(timezone.utc).strftime("%Y-%m-%d"))
        elif isinstance(d, date):
            out.add(d.strftime("%Y-%m-%d"))
        else:
            # last-ditch: stringification
            s = str(d).strip()
            if len(s) >= 10:
                s10 = s[:10]
                if _DATE_RE.match(s10):
                    out.add(s10)

    return sorted(out)


def get_available_dates_for_survey(vessel: str, survey: str) -> list[str]:
    """
    Pull available dates for (vessel, survey) from the cache utility.
    Expected function (per your note):
        get_dates_of_survey_in_ncei_cache(...)
    """
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
    Cursor starts at first option (no default passed).
    """
    choices = [{"name": d, "value": d} for d in available_dates]
    return str(
        inquirer.select(
            message=f"📆   {label} date (available):",
            choices=choices,
            max_height="70%",
        ).execute()
    )


def pick_datetime_parts(label: str, available_dates: list[str]) -> tuple[str, str, datetime]:
    """
    Collect (date_str, time_str, dt_utc) where:
      date_str = YYYY-MM-DD (must be in available_dates)
      time_str = HH:MM:SS (manual validated)
      dt_utc    = timezone-aware datetime in UTC

    This respects "world time / unix time" by constructing an actual UTC datetime.
    """
    date_str = pick_date_from_available(label, available_dates)
    hh, mm, ss = pick_time_text(label, default="00:00:00")
    y, m, d = (int(date_str[0:4]), int(date_str[5:7]), int(date_str[8:10]))
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
    """
    Schema target:
      - start-date: "YYYY-MM-DD"   (must be available for survey)
        start-time: "HH:MM:SS"     (manual validated)
        end-date:   "YYYY-MM-DD"   (must be available for survey)
        end-time:   "HH:MM:SS"     (manual validated)
    """
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


def create_time_window(*, available_dates: list[str]) -> TimeWindow:
    """
    Prompt for Start (date from available list + time via text) and End (same),
    validate end > start (UTC).
    """
    while True:
        try:
            s_date, s_time, s_dt = pick_datetime_parts("Start", available_dates)
            e_date, e_time, e_dt = pick_datetime_parts("End", available_dates)
        except ValueError as e:
            print(f"\n🚫 {e}\n")
            continue

        if e_dt <= s_dt:
            print("\n🚫 End must be after Start. Please try again.\n")
            continue

        return TimeWindow(
            start_date=s_date,
            start_time=s_time,
            end_date=e_date,
            end_time=e_time,
        )


def build_request() -> Request:
    vessel = choose_vessel()
    survey = choose_survey(vessel)
    instrument = choose_instrument(vessel, survey)

    # NEW: constrain date picking to available survey dates
    available_dates = get_available_dates_for_survey(vessel, survey)

    windows: list[TimeWindow] = []
    while True:
        windows.append(create_time_window(available_dates=available_dates))

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

    # Emit YAML schema:
    # requests:
    #   - vessel: "Falkor"
    #     survey: "FK004E"
    #     instrument: "EM302"
    #     time-windows:
    #       - start-date: "2012-08-29"
    #         start-time: "00:00:00"
    #         end-date: "2012-09-01"
    #         end-time: "23:59:59"
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