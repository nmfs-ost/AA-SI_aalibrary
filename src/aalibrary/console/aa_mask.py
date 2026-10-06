#!/usr/bin/env python3
"""
aa-mask

Apply one or more masks to Sv (or MVBS, TS): the Echoview "Mask" operator.
Samples a mask removes become NaN (no data); everything else is unchanged.

Each mask is given with what it means:

    --remove FILE   remove the samples where the mask is True
                    (noise masks: impulse, transient, attenuated, seafloor)
    --keep FILE     keep only the samples where the mask is True
                    (selections: shoals, frequency differencing, regions)
    --mask FILE     whichever the mask's own variable says (the aa-* masks
                    are known by name); an unknown mask must be given as
                    --remove or --keep

Several masks combine: a sample survives when no --remove mask is True there
and every --keep mask is True there.
"""

from __future__ import annotations

import logging
import sys
import warnings

logging.disable(logging.CRITICAL)
warnings.filterwarnings("ignore")

from loguru import logger  # noqa: E402

logger.remove()
logger.add(sys.stderr, level="WARNING")

import argparse  # noqa: E402

from aalibrary.console._core import (  # noqa: E402
    Help, Run, ToolSpec, add_common_flags, canon, render, show_help, stdio,
)

#: What the aa-* masks mean, by the variable they are written in.
#: "remove": True marks samples to drop. "keep": True marks samples to keep.
MEANINGS: dict[str, str] = {
    "impulse_mask": "remove",        # aa-impulse: True = impulse noise
    "transient_mask": "remove",      # aa-transient: True = transient noise
    "attenuated_mask": "remove",     # aa-attenuated: True = attenuated ping
    "seafloor_mask": "remove",       # aa-detect-seafloor --emit-mask: True = below bottom
    "transient_detect_mask": "keep", # aa-detect-transient: True = VALID
    "shoal_mask": "keep",            # aa-detect-shoal: True = inside a shoal
    "freqdiff_mask": "keep",         # aa-freqdiff: True = criterion holds
    "region_mask": "keep",           # aa-evr --write-mask: True = inside a region
}
#: Tools whose mask is stored under another name ("Sv" for aa-min).
TOOL_MEANINGS = {"aa-min": "remove"}

SPEC = ToolSpec(
    name="aa-mask",
    role="transform",
    kind="sv",
    op="aa_mask.apply",
    op_version=1,
    # The masks are inputs (role "mask"): their identity enters the hash. What
    # each one means (remove/keep), in input order, is the parameter.
    params={"var": canon.text},
)

HELP = Help(
    summary="Apply masks to Sv: remove noise, or keep only a selection (Echoview's Mask).",
    does=(
        "Reads the masks' boolean variable (impulse_mask, shoal_mask, ...; NaN counts "
        "as False) and sets every (ping_time x range) variable of the input to NaN "
        "where a sample is removed. Axis variables (echo_range, depth, ...) are left "
        "alone. A mask without a channel dimension applies to every channel; one with "
        "channels is matched by channel.\n\n"
        "A sample survives when no --remove mask is True there and every --keep mask "
        "is True there. --mask uses the meaning of the aa-* mask variables "
        "(impulse/transient/attenuated/seafloor: remove; shoal/freqdiff/region and "
        "aa-detect-transient's VALID mask: keep)."
    ),
    stdin="One Sv / MVBS / TS product (.nc or .zarr) path or gs:// URI.",
    stdout="The masked product's path (or gs:// URI).",
    options=[
        ("--remove FILE", "drop samples where this mask is True; repeatable"),
        ("--keep FILE", "keep only samples where this mask is True; repeatable"),
        ("--mask FILE", "use the mask's own meaning (aa-* masks); repeatable"),
        ("--var NAME", "the mask's variable, when the file has several"),
        ("-o, --output_path PATH", "exact output; local or gs://"),
    ],
    science={"var": "The mask variable read (when given)."},
    files=(
        "Reads NetCDF or Zarr, local or gs://; masks too. Writes <base>_<hash8>.nc "
        "beside the input (current directory for gs:// input), or -o, or --dest. "
        "The masks are recorded as inputs and their content is in the hash."
    ),
    pipeline="... | aa-sv | aa-mask --remove impulse.nc --remove transient.nc | aa-mvbs",
    examples=[
        "aa-mask sv.nc --mask sv_impulse.nc --mask sv_attenuated.nc",
        "aa-mask sv.nc --keep krill_freqdiff.nc",
    ],
)


def print_help():
    sys.stdout.write(render(SPEC, HELP, _build_parser()))


def _build_parser():
    p = argparse.ArgumentParser(prog="aa-mask", add_help=False,
                                description="Apply masks to Sv.")
    p.add_argument("input_path", type=str, nargs="?")
    p.add_argument("-o", "--output_path", type=str, default=None)
    p.add_argument("--remove", action="append", default=None, metavar="FILE")
    p.add_argument("--keep", action="append", default=None, metavar="FILE")
    p.add_argument("--mask", action="append", default=None, metavar="FILE")
    p.add_argument("--var", type=str, default=None)
    p.add_argument("--quiet", action="store_true")
    add_common_flags(p)
    return p


def mask_variable(ds, var: str | None = None) -> str:
    if var:
        if var not in ds.data_vars:
            raise ValueError(f"no variable {var!r} in the mask (have: {', '.join(ds.data_vars)})")
        return var
    known = [name for name in MEANINGS if name in ds.data_vars]
    if known:
        return known[0]
    boolean = [name for name, da in ds.data_vars.items() if da.dtype == bool]
    if len(boolean) == 1:
        return boolean[0]
    if "Sv" in ds.data_vars:
        return "Sv"
    raise ValueError("cannot tell which variable is the mask; give --var")


def meaning_of(ds, name: str, tool: str = "") -> str | None:
    return MEANINGS.get(name) or TOOL_MEANINGS.get(tool)


def _as_bool(da):
    import numpy as np

    if da.dtype == bool:
        return da
    return (da.fillna(0) != 0) if np.issubdtype(da.dtype, np.number) else da.astype(bool)


def align(mask, target):
    """The mask on the target's (channel?, time, range) grid, positionally.

    Masks made from the same Sv share its grid; a different shape is an error,
    not something to interpolate.
    """
    import xarray as xr

    from aalibrary.console import _flat

    ttime, trange = _flat.time_dim(target), _flat.range_dim(target)
    mtime, mrange = _flat.time_dim(mask), _flat.range_dim(mask)
    if mtime is None or mrange is None:
        raise ValueError(f"the mask has no ping_time/range dimensions: {mask.dims}")
    if mask.sizes[mtime] != target.sizes[ttime] or mask.sizes[mrange] != target.sizes[trange]:
        raise ValueError(
            f"the mask is {mask.sizes[mtime]} pings x {mask.sizes[mrange]} samples; the data "
            f"is {target.sizes[ttime]} x {target.sizes[trange]}: make the mask from this "
            "product (or one on the same grid)")
    m = mask
    if "channel" in m.dims:
        if "channel" not in target.dims:
            raise ValueError("the mask has channels and the data does not")
        tch = [str(c) for c in target["channel"].values]
        mch = [str(c) for c in m["channel"].values]
        if set(tch) <= set(mch):
            m = m.sel(channel=target["channel"].values)
        elif len(mch) == len(tch):
            m = m.assign_coords(channel=target["channel"].values)
        else:
            raise ValueError("the mask's channels do not match the data's")
        dims = ("channel", mtime, mrange)
    else:
        dims = (mtime, mrange)
    data = _as_bool(m).transpose(*dims).data
    out_dims = tuple({"channel": "channel", mtime: ttime, mrange: trange}[d] for d in dims)
    return xr.DataArray(data, dims=out_dims)


def apply(ds, masks: list[tuple[str, object]]):
    """ds with every gridded variable NaN where a sample does not survive."""
    import numpy as np

    from aalibrary.console import _flat

    names = _flat.gridded(ds)
    if not names:
        raise ValueError("the input has no (ping_time x range) variable to mask")
    out = ds.copy()
    for name in names:
        da = ds[name]
        keep = None
        for meaning, m in masks:
            aligned = align(m, da)
            ok = ~aligned if meaning == "remove" else aligned
            keep = ok if keep is None else (keep & ok)
        if keep is None:
            continue
        masked = da.where(keep, np.nan).transpose(*da.dims)
        masked.attrs = dict(da.attrs)
        out[name] = masked
    return out


def main() -> None:
    if len(sys.argv) == 1 and not stdio.stdin_is_piped():
        print_help()
        sys.exit(0)
    parser = _build_parser()
    if show_help(SPEC, HELP, parser):
        sys.exit(0)
    args = parser.parse_args()
    given = ([("remove", f) for f in args.remove or []]
             + [("keep", f) for f in args.keep or []]
             + [("auto", f) for f in args.mask or []])
    if not given:
        print("aa-mask: give at least one --remove, --keep or --mask", file=sys.stderr)
        sys.exit(2)

    from aalibrary.console import _flat

    token = stdio.one_input(args.input_path, SPEC.name)
    probe = Run(SPEC, args)
    src = probe.input(token)
    loaded = []
    meanings = []
    for how, path in given:
        inp = probe.param_file(path, role="mask")
        try:
            mds = _flat.open_flat(inp.local, chunks={})
            var = mask_variable(mds, args.var)
        except (ValueError, OSError) as exc:
            print(f"aa-mask: {path}: {exc}", file=sys.stderr)
            sys.exit(1)
        tool = ((inp.prov or {}).get("pipeline") or [{}])[-1].get("tool", "")
        meaning = how if how != "auto" else meaning_of(mds, var, tool)
        if meaning is None:
            print(f"aa-mask: {path}: '{var}' is not an aa-* mask whose meaning is known; "
                  "give it as --remove or --keep", file=sys.stderr)
            sys.exit(2)
        loaded.append((meaning, mds[var]))
        meanings.append({"meaning": meaning, "var": var})

    run = Run(SPEC, args, params={"masks": meanings})
    run.input(token)
    for _how, path in given:
        run.param_file(path, role="mask")
    out = run.plan(ext=".nc", explicit=args.output_path or None, kind=_flat.kind_of(src))
    if run.reusable(out):
        run.finish(out)
        return
    try:
        ds = _flat.open_flat(src.local, chunks={})
        result = apply(ds, loaded)
        _flat.write_netcdf(result, out.local)
    except ValueError as exc:
        run.discard(out)
        print(f"aa-mask: {exc}", file=sys.stderr)
        sys.exit(1)
    except Exception as exc:  # noqa: BLE001
        run.discard(out)
        logger.exception(f"aa-mask: {exc}")
        sys.exit(1)
    run.finish(out)


if __name__ == "__main__":
    main()
