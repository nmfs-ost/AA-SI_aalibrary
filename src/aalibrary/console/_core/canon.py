"""Canonical forms for scientific parameters.

A product hash must not depend on *how* an option was typed, only on what
it means. argparse already removes flag order and alias spellings
(`--range_bin` and `--range-bin` land in the same `dest`) and fills in
defaults, so an explicit default hashes the same as an omitted one. What
is left is value spelling, which the canonicalizers here normalize:

    "10dB", "10.0 dB", "10.0db"  -> "10dB"
    20, 20.0, "20"               -> 20
    -0.0                         -> 0
    nan                          -> "NaN"
    ["b=2", "a=1"]               -> {"a": 1, "b": 2}

Each tool declares, per argparse `dest`, which canonicalizer applies (see
``ToolSpec.params``). Anything not declared is not scientific and never
reaches the hash.

``dumps`` is the one serializer used for hashing: sorted keys, no
whitespace, no NaN/Infinity literals. Change it and every hash changes.
"""

from __future__ import annotations

import ast
import json
import math
import re
from typing import Any, Callable, Iterable

Canon = Callable[[Any], Any]


# ---------------------------------------------------------------------------
# The serializer behind every hash.
# ---------------------------------------------------------------------------

def normalize(value: Any) -> Any:
    """Recursively make a value JSON-safe and spelling-independent."""
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, (int, float)):
        return number(value)
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        return {str(k): normalize(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [normalize(v) for v in value]
    if isinstance(value, (set, frozenset)):
        return sorted((normalize(v) for v in value), key=dumps)
    # numpy scalars and friends
    item = getattr(value, "item", None)
    if callable(item):
        try:
            return normalize(item())
        except Exception:  # pragma: no cover - defensive
            pass
    return str(value)


def dumps(value: Any) -> str:
    """Canonical JSON: the exact bytes that get hashed."""
    return json.dumps(
        normalize(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


# ---------------------------------------------------------------------------
# Canonicalizers. Each takes the parsed argparse value (or None) and returns
# a JSON-safe canonical value. None always stays None ("not given").
# ---------------------------------------------------------------------------

def number(value: Any) -> Any:
    """20 == 20.0 == "20"; -0.0 == 0; NaN/inf become strings."""
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value          # exact, however large (float() would round past 2**53)
    if isinstance(value, str):
        text = value.strip()
        if text.lstrip("+-").isdigit():
            return int(text)
    x = float(value)
    if math.isnan(x):
        return "NaN"
    if math.isinf(x):
        return "Infinity" if x > 0 else "-Infinity"
    if x == 0:
        return 0
    if x.is_integer() and abs(x) < 2**53:
        return int(x)
    return x


def integer(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return int(str(value).strip()) if isinstance(value, str) else int(value)


def boolean(value: Any) -> Any:
    return None if value is None else bool(value)


def text(value: Any) -> Any:
    return None if value is None else str(value).strip()


def choice(case: str | None = None) -> Canon:
    """Enumerated string, optionally case-folded ('upper' or 'lower')."""

    def _canon(value: Any) -> Any:
        if value is None:
            return None
        s = str(value).strip()
        if case == "upper":
            return s.upper()
        if case == "lower":
            return s.lower()
        return s

    return _canon


_UNIT_ALIASES = {
    "db": "dB",
    "m": "m",
    "meter": "m",
    "meters": "m",
    "nmi": "nmi",
    "s": "s",
    "sec": "s",
    "min": "min",
    "h": "h",
    "hz": "Hz",
    "khz": "kHz",
}
_QUANTITY = re.compile(r"^\s*([-+]?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?)\s*([A-Za-z]*)\s*$")


def quantity(default_unit: str | None = None) -> Canon:
    """Number with a unit, as echopype spells them: '10.0 dB' -> '10dB'.

    A bare number gets ``default_unit``. Unrecognized strings are kept
    verbatim (stripped) rather than guessed at.
    """

    def _canon(value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            n, unit = number(value), default_unit or ""
            return f"{n}{unit}"
        m = _QUANTITY.match(str(value))
        if not m:
            return str(value).strip()
        n = number(m.group(1))
        unit = m.group(2)
        unit = _UNIT_ALIASES.get(unit.lower(), unit) if unit else (default_unit or "")
        return f"{n}{unit}"

    return _canon


def literal(value: Any) -> Any:
    """Python-literal text ('25', '25.0', '(1, 2)', 'True') to a canonical value."""
    if value is None:
        return None
    if not isinstance(value, str):
        return normalize(value)
    try:
        parsed = ast.literal_eval(value.strip())
    except Exception:
        return value.strip()
    if isinstance(parsed, tuple):
        parsed = list(parsed)
    return normalize(parsed)


def kv(value_canon: Canon = literal) -> Canon:
    """A list of 'key=value' strings (or a dict) to a key-sorted dict.

    Later duplicates win, as they do in every tool that parses these.
    """

    def _canon(value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, dict):
            items = value.items()
        else:
            items = []
            for item in _iter(value):
                if "=" not in str(item):
                    items.append((str(item).strip(), None))
                    continue
                k, v = str(item).split("=", 1)
                items.append((k.strip(), v))
        out: dict[str, Any] = {}
        for k, v in items:
            out[str(k)] = value_canon(v)
        return out or None

    return _canon


def set_of(item_canon: Canon = text) -> Canon:
    """Order-free collection (duplicates collapse)."""

    def _canon(value: Any) -> Any:
        if value is None:
            return None
        items = {dumps(item_canon(v)): item_canon(v) for v in _iter(value)}
        return [items[k] for k in sorted(items)] or None

    return _canon


def ordered(item_canon: Canon = text) -> Canon:
    """Order-significant list (e.g. channel selection, which sets output order)."""

    def _canon(value: Any) -> Any:
        if value is None:
            return None
        return [item_canon(v) for v in _iter(value)] or None

    return _canon


def csv_numbers(value: Any) -> Any:
    """'38000, 120000' -> [38000, 120000] (order kept)."""
    if value is None:
        return None
    parts = value if isinstance(value, (list, tuple)) else str(value).split(",")
    return [number(p) for p in parts if str(p).strip()]


_EXPR_NUM = re.compile(r"(?<![A-Za-z_])([-+]?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?)")


_QUOTED = re.compile(r'("[^"]*")')


def _expression_part(s: str) -> str:
    s = re.sub(r"\s+", "", s).replace("db", "dB").replace("DB", "dB")
    return _EXPR_NUM.sub(lambda m: str(number(m.group(1))), s)


def expression(value: Any) -> Any:
    """Arithmetic/comparison text: whitespace removed, numbers canonical.

    '38.0kHz - 120kHz >= 10.0 dB' -> '38kHz-120kHz>=10dB'. Text inside
    double quotes (channel names) is kept exactly: it is matched verbatim
    downstream, so its spaces and zeros are significant.
    """
    if value is None:
        return None
    parts = _QUOTED.split(str(value))
    return "".join(p if len(p) >= 2 and p[0] == p[-1] == '"' else _expression_part(p)
                   for p in parts)


def _iter(value: Any) -> Iterable[Any]:
    if isinstance(value, (list, tuple, set, frozenset)):
        return value
    if isinstance(value, str) and "," in value:
        return [v for v in value.split(",") if v.strip()]
    return [value]


__all__ = [
    "Canon", "normalize", "dumps", "number", "integer", "boolean", "text",
    "choice", "quantity", "literal", "kv", "set_of", "ordered", "csv_numbers",
    "expression",
]
