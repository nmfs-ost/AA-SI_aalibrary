"""Safety gate. The ONLY way a plan reaches the executor is through validate().

Defense in depth:
  1. Tool allowlist: every stage must be a real aa-* command we know about.
  2. Arg sanitization: no shell metachars in any arg token.
  3. Network/cloud access requires explicit user confirmation in the UI:
     a stage is flagged when its tool always or usually touches the network
     (NETWORK_TOOLS) or when any of its arguments is a remote URI (gs://,
     s3://, http(s)://), because every aa-* tool now reads and writes gs://.
     This module only flags; the UI decides what to do.
(Per-tool flag checking is not done here: each tool's own argparse rejects
unknown flags before it does any work.)
"""
from __future__ import annotations

import re
from dataclasses import dataclass

from .plan import Plan, PipelineStage


# Every console script in pyproject.toml [project.scripts], and nothing else.
# Keep in sync when a tool is added or removed.
# aa-crop is deliberately absent: its module exists but it has no entry point.
KNOWN_TOOLS: frozenset[str] = frozenset({
    # find and fetch data
    "aa-find", "aa-raw", "aa-request", "aa-get", "aa-fetch", "aa-download",
    "aa-sonar",
    # make EchoData
    "aa-nc", "aa-ed", "aa-combine",
    # calibrate and add coordinates
    "aa-sv", "aa-ts", "aa-depth", "aa-location", "aa-splitbeam-angle",
    "aa-swap-freq", "aa-coerce-time",
    # noise, masks and lines
    "aa-clean", "aa-noise-est", "aa-impulse", "aa-min", "aa-transient",
    "aa-attenuated", "aa-detect-transient", "aa-detect-shoal",
    "aa-detect-seafloor", "aa-freqdiff", "aa-evl", "aa-evr",
    # select, mask, lines and regions, calibration files
    "aa-crop", "aa-mask", "aa-threshold", "aa-annotate", "aa-ecs",
    # grid and integrate; echometrics
    "aa-mvbs", "aa-mvbs-index", "aa-nasc", "aa-integrate",
    "aa-abundance", "aa-aggregation", "aa-center-of-mass", "aa-dispersion",
    "aa-evenness",
    # seawater
    "aa-sound-speed", "aa-absorption",
    # look and check
    "aa-graph", "aa-plot", "aa-tiles", "aa-show", "aa-metadata", "aa-store",
    # store and share
    "aa-upload", "aa-cruisepack",
    # set up and help
    "aa-guide", "aa-help", "aa-setup", "aa-refresh", "aa-test",
})

# Tools that always, or in their usual use, reach the network or cloud
# storage. A plan with one of these gets an extra confirmation prompt.
#   aa-raw        downloads from NCEI (optionally uploads to GCP)
#   aa-fetch      queries the metadata database (BigQuery) and downloads
#   aa-ed         given a bare file name: BigQuery lookup + NCEI download;
#                 --gcs-uri/--gcs-prefix read and write a bucket (only a
#                 local .raw or directory is offline, but the planner mostly
#                 uses it for the NCEI case, so it is always flagged)
#   aa-find       browses NCEI / S3
#   aa-get        its menus list vessels/surveys/instruments from the NCEI
#                 metadata cache in BigQuery (ncei_cache_utils)
#   aa-download   gs:// -> local
#   aa-upload     local -> GCS (gs:// destination or the older modes)
#   aa-cruisepack uploads CruisePack databases to GCS (and may pip-install
#                 missing packages)
#   aa-help       calls Vertex AI
#   aa-refresh    pip-installs from GitHub
#   aa-setup      downloads and runs the workstation setup script, gcloud login
# Not listed because they are offline unless given a remote URI (which
# _remote_args() catches): aa-request only writes a YAML file from flags;
# aa-combine, aa-store, aa-metadata, aa-evr and every product tool read and
# write gs:// only when an argument says so.
NETWORK_TOOLS: frozenset[str] = frozenset({
    "aa-raw", "aa-fetch", "aa-ed", "aa-find", "aa-get", "aa-download",
    "aa-upload", "aa-cruisepack", "aa-help", "aa-refresh", "aa-setup",
})

# Interactive or setup tools: they need the terminal (menus, prompts, a
# browser) or change the installation, so the executor's pipes can't drive
# them. A plan may still name them (e.g. a single-stage plan), but a warning
# asks the user to run them on their own. aa-get is meant to start a shell
# pipe (aa-get | aa-fetch: its menus go to the terminal through stderr), but
# the executor pipes every stage's stderr, so as the first of several stages
# it finds no terminal and refuses; alone it works.
STANDALONE_TOOLS: frozenset[str] = frozenset({
    "aa-find", "aa-get", "aa-help", "aa-cruisepack", "aa-setup", "aa-refresh",
})

# An argument that names a remote object: gs://bucket/key, s3://..., https://...
_REMOTE_ARG = re.compile(r"(?:^|=)(?:gs|s3|gcs|https?)://", re.IGNORECASE)


def _remote_args(stage: PipelineStage) -> list[str]:
    """Arguments of a stage that point at remote storage."""
    return [a for a in stage.args if isinstance(a, str) and _REMOTE_ARG.search(a)]

# Characters that have no business being in an argv token. The point isn't
# perfect parsing -- subprocess never sees a shell, so an arg literally
# containing `;` or `|` would just be passed verbatim to the tool and almost
# certainly cause it to error out. We block these anyway because their
# presence is a strong signal the planner hallucinated a shell snippet
# instead of a real argv list, and we'd rather catch that early than ship
# garbage args to a real subprocess.
_FORBIDDEN_ARG_CHARS = re.compile(r"[`\n\r\x00]")
_SHELL_METACHAR_PATTERNS = (
    re.compile(r";"),
    re.compile(r"&&"),
    re.compile(r"\|\|"),
    re.compile(r"(?<!\d)\|(?!\d)"),   # bare pipe, but not "|something|" inside e.g. a regex literal
    re.compile(r"&(?!\d)"),
    re.compile(r"\$\("),
    re.compile(r"\$\{"),
    re.compile(r"^>"),                # leading redirect
    re.compile(r"\s>"),               # space-then-redirect
    re.compile(r"\s<"),
)
_REDIRECT_PATTERNS = _SHELL_METACHAR_PATTERNS[-3:]

# Options whose value is a comparison expression, where "<" and ">" are the
# operator, not a redirect: aa-freqdiff --freqABEq '120kHz - 38kHz > 2dB'.
# Their values skip the redirect patterns (never the others).
_EXPRESSION_FLAGS = frozenset({"--freqABEq", "--chanABEq"})


def _is_expression_value(prev: str | None, tok: str) -> bool:
    if prev in _EXPRESSION_FLAGS:
        return True
    return any(tok.startswith(flag + "=") for flag in _EXPRESSION_FLAGS)


@dataclass
class ValidationResult:
    ok: bool
    errors: list[str]            # hard blockers -- never run
    warnings: list[str]          # soft -- user must confirm
    needs_network_confirm: bool  # any stage uses a NETWORK_TOOL


def validate(plan: Plan) -> ValidationResult:
    errors: list[str] = []
    warnings: list[str] = []
    needs_net = False

    if plan.kind != "pipeline":
        # Non-pipeline plans don't get executed at all; nothing to validate.
        return ValidationResult(ok=True, errors=[], warnings=[],
                                needs_network_confirm=False)

    if not plan.stages:
        errors.append("Plan has kind=pipeline but no stages.")
        return ValidationResult(ok=False, errors=errors, warnings=warnings,
                                needs_network_confirm=False)

    for i, stage in enumerate(plan.stages):
        prefix = f"Stage {i + 1} ({stage.tool or '?'}):"

        if not stage.tool:
            errors.append(f"{prefix} missing tool name.")
            continue
        if stage.tool not in KNOWN_TOOLS:
            errors.append(f"{prefix} '{stage.tool}' is not a known aa-* tool.")
            continue
        if stage.tool in NETWORK_TOOLS:
            needs_net = True
            warnings.append(
                f"{prefix} {stage.tool} accesses network/cloud storage."
            )
        else:
            remote = _remote_args(stage)
            if remote:
                needs_net = True
                warnings.append(
                    f"{prefix} reads or writes remote storage ({remote[0]})."
                )
        if stage.tool in STANDALONE_TOOLS and len(plan.stages) > 1:
            if stage.tool == "aa-get":
                warnings.append(
                    f"{prefix} aa-get draws its menus on the terminal, which "
                    "aa-help's runner doesn't give it inside a pipeline. Run "
                    "`aa-get` on its own, then `aa-fetch <path>` (in a shell, "
                    "`aa-get | aa-fetch` works)."
                )
            else:
                warnings.append(
                    f"{prefix} {stage.tool} is interactive or a setup tool; "
                    "aa-help's runner can't drive it inside a pipeline. Run it "
                    "on its own."
                )

        for j, tok in enumerate(stage.args):
            prev_tok = stage.args[j - 1] if j else None
            if not isinstance(tok, str):
                errors.append(f"{prefix} non-string arg {tok!r}.")
                continue
            if _FORBIDDEN_ARG_CHARS.search(tok):
                errors.append(
                    f"{prefix} arg {tok!r} contains forbidden control chars."
                )
                continue
            expression = _is_expression_value(
                prev_tok if isinstance(prev_tok, str) else None, tok)
            for pat in _SHELL_METACHAR_PATTERNS:
                if expression and pat in _REDIRECT_PATTERNS:
                    continue
                if pat.search(tok):
                    errors.append(
                        f"{prefix} arg {tok!r} contains shell metacharacters. "
                        "The runner handles piping itself; the planner should "
                        "emit clean argv tokens."
                    )
                    break

    return ValidationResult(
        ok=not errors,
        errors=errors,
        warnings=warnings,
        needs_network_confirm=needs_net,
    )


def render_command(stage: PipelineStage) -> str:
    """Render a stage as a copy-pasteable shell command (display only)."""
    import shlex
    parts = [stage.tool] + [shlex.quote(a) for a in stage.args]
    return " ".join(parts)


def render_pipeline(plan: Plan) -> str:
    """Render the whole plan as a multi-line shell pipeline (display only)."""
    if plan.kind != "pipeline" or not plan.stages:
        return ""
    cmds = [render_command(s) for s in plan.stages]
    if len(cmds) == 1:
        return cmds[0]
    return " \\\n  | ".join(cmds)
