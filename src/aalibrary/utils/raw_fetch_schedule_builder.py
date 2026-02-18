#!/usr/bin/env python3
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import calendar
import random
from typing import Any

from InquirerPy import inquirer

try:
    import yaml  # type: ignore
except Exception:
    yaml = None


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


def pick_datetime(label: str, *, year_min: int = 1970, year_max: int = 2100) -> datetime:
    year = select_int(f"📅   {label} year:", list(range(year_min, year_max + 1)))

    month_choices = [
        {"name": f"{i:02d} - {calendar.month_name[i]}", "value": i}
        for i in range(1, 13)
    ]
    month = int(select_value(f"🗓️   {label} month:", month_choices))

    dim = days_in_month(year, month)
    day = select_int(f"📆   {label} day:", list(range(1, dim + 1)))

    hour = select_int(f"🕒   {label} hour:", list(range(0, 24)))
    minute = select_int(f"⏱️   {label} minute:", list(range(0, 60)))
    second = select_int(f"⏲️   {label} second:", list(range(0, 60)))

    return datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)


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


def choose_vessel() -> str:
    vessels = get_vessel_names()
    return str(
        inquirer.fuzzy(
            message="🛥️   Select vessel (type to search):",
            choices=vessels,
            max_height="70%",
        ).execute()
    )


def choose_survey(vessel: str) -> str:
    surveys = get_fake_surveys_for_vessel(vessel, n=60)
    return str(
        inquirer.fuzzy(
            message=f"📋   Select survey for {vessel} (type to search):",
            choices=surveys,
            max_height="70%",
        ).execute()
    )


def choose_instrument() -> str:
    instruments = get_instruments()
    return str(
        inquirer.select(
            message="🎛️   Select instrument:",
            choices=instruments,
            max_height="70%",
        ).execute()
    )


def create_time_window() -> TimeWindow:
    while True:
        start_dt = pick_datetime("Start")
        end_dt = pick_datetime("End")

        if end_dt <= start_dt:
            print("\n🚫 End must be after Start. Please try again.\n")
            continue

        return TimeWindow(start=to_utc_z(start_dt), end=to_utc_z(end_dt))


def build_request() -> Request:
    vessel = choose_vessel()
    survey = choose_survey(vessel)
    instrument = choose_instrument()

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
