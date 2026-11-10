"""How the console tools chain: what each reads, writes, adds and needs.

argparse says what a tool accepts; it cannot say what a tool *reads* (an Sv,
not an EchoData), what it adds to its input (depth, position), what the input
must already carry, or which options take another product (an ECS, a line
file, a mask). Front ends that build pipelines (the AA-SI Workbench) read it
here, from the library that has the tools, through ``--describe`` or
:func:`table`, instead of keeping their own copy.

Kinds of product and their data levels follow echopype's Echolevels: L0 raw,
L1 EchoData, L2A calibrated, L2B derived from calibrated, L3 gridded and
integrated, L4 metrics. Annotations (lines, regions) and calibration files are
inputs to processing, not levels of it; renderings have no level.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field

SCHEMA = "aa-chaining/1"

KINDS: dict[str, tuple[str, str]] = {
    # kind: (label, level)
    "raw": ("Raw", "L0"),
    "echodata": ("EchoData", "L1"),
    "sv": ("Sv", "L2A"),
    "ts": ("TS", "L2A"),
    "mask": ("Mask", "L2B"),
    "seafloor": ("Seafloor line", "L2B"),
    "noise": ("Noise", "L2B"),
    "mvbs": ("MVBS", "L3"),
    "nasc": ("NASC", "L3"),
    "integration": ("Integration", "L3"),
    "echometric": ("Echometric", "L4"),
    "lines": ("Lines", ""),
    "regions": ("Regions", ""),
    "calibration": ("Calibration", ""),
    "echogram": ("Echogram", ""),
    "html": ("Interactive plot", ""),
    "tiles": ("Echogram tiles", ""),
}

GROUPS = (
    "Calibrate",
    "Clean and correct",
    "Select and mask",
    "Add variables",
    "Grid and integrate",
    "Masks and detection",
    "Lines and regions",
    "Echometrics",
    "Render",
)

#: Variables a product can carry that some tools need, and the tool adding each.
FEATURES = {"depth": "aa-depth", "location": "aa-location", "angles": "aa-splitbeam-angle"}


@dataclass(frozen=True)
class Traits:
    group: str
    label: str
    consumes: tuple[str, ...]
    produces: str
    #: Variables the input must have.
    needs: tuple[str, ...] = ()
    #: Needs depth only when this option's value is "depth".
    depth_param: str = ""
    #: Variables this tool adds (it passes its input's on).
    adds: tuple[str, ...] = ()
    #: The option that takes the EchoData the input was made from.
    echodata_flag: str = ""
    #: ... which the tool cannot do without,
    echodata_required: bool = False
    #: ... or needs only when one of these options is on,
    echodata_when: tuple[str, ...] = ()
    #: ... or uses when it is known (a track for positions) and does without.
    echodata_optional: bool = False
    #: Options shown first, in this order.
    primary: tuple[str, ...] = ()
    #: Writes the same kind of product it reads (a correction or selection).
    passthrough: bool = False
    #: Options that take another product, and the kinds they take.
    inputs: dict = field(default_factory=dict)
    #: Of those, the ones the tool cannot run without.
    required_inputs: tuple[str, ...] = ()


SV_LIKE = ("sv",)
GRIDDED = ("sv", "mvbs", "ts")

TRAITS: dict[str, Traits] = {
    # Calibrate
    "aa-sv": Traits("Calibrate", "Sv", ("echodata",), "sv",
                    primary=("waveform_mode", "encode_mode", "ecs"),
                    inputs={"ecs": ("calibration",)}),
    "aa-ts": Traits("Calibrate", "TS", ("echodata",), "ts",
                    primary=("waveform_mode", "encode_mode", "ecs"),
                    inputs={"ecs": ("calibration",)}),
    # Clean and correct
    "aa-clean": Traits("Clean and correct", "Remove noise", SV_LIKE, "sv",
                       primary=("snr_threshold", "ping_num", "range_sample_num")),
    "aa-coerce-time": Traits("Clean and correct", "Fix time order", ("sv", "mvbs"), "sv",
                             passthrough=True),
    "aa-swap-freq": Traits("Clean and correct", "Index by frequency", ("sv", "mvbs"), "sv",
                           passthrough=True),
    # Select and mask
    "aa-crop": Traits("Select and mask", "Crop", ("sv", "mvbs", "ts", "mask"), "sv",
                      passthrough=True,
                      primary=("start", "end", "min_range", "max_range", "frequency")),
    "aa-threshold": Traits("Select and mask", "Threshold", GRIDDED, "sv", passthrough=True,
                           primary=("min", "max", "below", "above")),
    "aa-mask": Traits("Select and mask", "Apply masks", GRIDDED, "sv", passthrough=True,
                      primary=("remove", "keep", "mask"),
                      inputs={"remove": ("mask",), "keep": ("mask",), "mask": ("mask",)}),
    "aa-evl": Traits("Select and mask", "Exclude by lines", ("sv", "mvbs"), "sv",
                     passthrough=True, primary=("evl", "keep", "depth_offset"),
                     inputs={"evl": ("lines",)}, required_inputs=("evl",)),
    "aa-evr": Traits("Select and mask", "Keep regions", ("sv", "mvbs"), "sv",
                     passthrough=True, primary=("evr",),
                     inputs={"evr": ("regions",)}, required_inputs=("evr",)),
    # Add variables
    "aa-depth": Traits("Add variables", "Depth", SV_LIKE, "sv", adds=("depth",),
                       echodata_flag="--echodata",
                       echodata_when=("use_platform_vertical_offsets", "use_platform_angles",
                                      "use_beam_angles"),
                       primary=("depth_offset", "tilt", "downward")),
    "aa-location": Traits("Add variables", "Position", SV_LIKE, "sv", adds=("location",),
                          echodata_flag="--echodata", echodata_required=True),
    "aa-splitbeam-angle": Traits("Add variables", "Split-beam angles", SV_LIKE, "sv",
                                 adds=("angles",), echodata_flag="--echodata",
                                 echodata_required=True,
                                 primary=("waveform_mode", "encode_mode")),
    # Grid and integrate
    "aa-mvbs": Traits("Grid and integrate", "MVBS", SV_LIKE, "mvbs", depth_param="range_var",
                      primary=("range_bin", "ping_time_bin", "range_var", "method")),
    "aa-mvbs-index": Traits("Grid and integrate", "MVBS by index", ("sv", "echodata"), "mvbs",
                            primary=("range_sample_num", "ping_num")),
    "aa-nasc": Traits("Grid and integrate", "NASC", SV_LIKE, "nasc",
                      needs=("depth", "location"), primary=("range_bin", "dist_bin")),
    "aa-integrate": Traits("Grid and integrate", "Integrate (Echoview)", SV_LIKE,
                           "integration", echodata_flag="--echodata", echodata_optional=True,
                           primary=("by", "interval", "layer", "min_sv", "bottom",
                                    "bottom_offset", "surface_depth", "regions", "bad"),
                           inputs={"surface": ("lines",), "bottom": ("lines", "seafloor"),
                                   "bad": ("regions",), "regions": ("regions",)}),
    # Masks and detection
    "aa-impulse": Traits("Masks and detection", "Impulse noise", SV_LIKE, "mask",
                         depth_param="range_var",
                         primary=("impulse_noise_threshold", "depth_bin", "range_var")),
    "aa-min": Traits("Masks and detection", "Impulse noise (min)", SV_LIKE, "mask",
                     depth_param="range_var"),
    "aa-transient": Traits("Masks and detection", "Transient noise", SV_LIKE, "mask",
                           depth_param="range_var"),
    "aa-attenuated": Traits("Masks and detection", "Attenuated signal", SV_LIKE, "mask",
                            depth_param="range_var"),
    "aa-freqdiff": Traits("Masks and detection", "Frequency difference", ("sv", "mvbs"),
                          "mask", primary=("freqABEq", "chanABEq")),
    "aa-noise-est": Traits("Masks and detection", "Noise estimate", ("sv", "echodata"),
                           "noise"),
    "aa-detect-seafloor": Traits("Masks and detection", "Seafloor", SV_LIKE, "seafloor",
                                 needs=("depth",), primary=("method", "param")),
    "aa-detect-shoal": Traits("Masks and detection", "Shoals", SV_LIKE, "mask",
                              primary=("method", "param")),
    "aa-detect-transient": Traits("Masks and detection", "Transient (detector)", SV_LIKE,
                                  "mask", depth_param="range_var",
                                  primary=("method", "param")),
    # Lines and regions
    "aa-annotate": Traits("Lines and regions", "Bottom as a line", ("seafloor",), "lines",
                          primary=("name", "tolerance")),
    # Echometrics
    "aa-abundance": Traits("Echometrics", "Abundance (Sa)", SV_LIKE, "echometric"),
    "aa-aggregation": Traits("Echometrics", "Aggregation", SV_LIKE, "echometric"),
    "aa-center-of-mass": Traits("Echometrics", "Center of mass", SV_LIKE, "echometric"),
    "aa-dispersion": Traits("Echometrics", "Dispersion", SV_LIKE, "echometric"),
    "aa-evenness": Traits("Echometrics", "Evenness", SV_LIKE, "echometric"),
    # Render
    "aa-graph": Traits("Render", "Echogram", ("sv", "mvbs", "nasc", "mask", "noise", "ts"),
                       "echogram", primary=("var", "vmin", "vmax", "cmap", "decimate")),
    "aa-plot": Traits("Render", "Interactive plot", ("sv", "mvbs", "mask", "noise"), "html",
                      primary=("var", "vmin", "vmax", "cmap")),
    "aa-tiles": Traits("Render", "Echogram tiles", ("sv", "mvbs", "mask", "noise", "ts"),
                       "tiles", echodata_flag="--echodata", echodata_optional=True,
                       primary=("var", "y", "reduce")),
}

#: The kind each product-writing tool records, including the tools that start
#: chains (EchoData builders) or make inputs (ECS) rather than sit in them.
TOOL_KIND: dict[str, str] = {
    "aa-nc": "echodata", "aa-ed": "echodata", "aa-combine": "echodata",
    "aa-ecs": "calibration",
    **{name: t.produces for name, t in TRAITS.items()},
}


def traits_of(tool: str) -> dict | None:
    t = TRAITS.get(tool)
    if t is None:
        return None
    out = asdict(t)
    out["inputs"] = {k: list(v) for k, v in t.inputs.items()}
    return out


def table() -> dict:
    """Everything above, as plain JSON-able data."""
    return {
        "schema": SCHEMA,
        "kinds": {k: {"label": label, "level": level} for k, (label, level) in KINDS.items()},
        "groups": list(GROUPS),
        "features": dict(FEATURES),
        "toolKind": dict(TOOL_KIND),
        "tools": {name: traits_of(name) for name in TRAITS},
    }


__all__ = ["SCHEMA", "KINDS", "GROUPS", "FEATURES", "Traits", "TRAITS", "TOOL_KIND",
           "traits_of", "table"]
