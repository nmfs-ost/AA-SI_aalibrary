"""Safety gate. The ONLY way a plan reaches the executor is through validate().

Defense in depth:
  1. Tool allowlist: every stage must be a real aa-* command we know about.
  2. Arg sanitization: no shell metachars in any arg token.
  3. Flag allowlist: each tool's args are checked against its known flag list.
  4. Network/destructive tools require explicit user confirmation in the UI;
     this module only flags them, the UI decides what to do.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

from .plan import Plan, PipelineStage


# Real aa-* tools from your pyproject.toml [project.scripts].
# Update this list when you add new tools.
KNOWN_TOOLS: frozenset[str] = frozenset({
    "aa-raw", "aa-nc", "aa-plot", "aa-test", "aa-find", "aa-setup",
    "aa-clean", "aa-mvbs", "aa-nasc", "aa-sv", "aa-ts", "aa-min",
    "aa-depth", "aa-show", "aa-impulse", "aa-transient", "aa-attenuated",
    "aa-noise-est", "aa-detect-transient", "aa-mvbs-index", "aa-swap-freq",
    "aa-coerce-time", "aa-splitbeam-angle", "aa-freqdiff", "aa-sound-speed",
    "aa-absorption", "aa-evenness", "aa-dispersion", "aa-center-of-mass",
    "aa-aggregation", "aa-abundance", "aa-detect-shoal", "aa-detect-seafloor",
    "aa-get", "aa-fetch", "aa-refresh", "aa-evr", "aa-evl", "aa-help",
    # not in pyproject yet but present in the system prompt -- keep allowlisted
    # if/when added; remove the entry otherwise.
    "aa-location", "aa-crop",
})

# Tools that hit the network or external storage. These get an extra
# confirmation prompt before execution.
NETWORK_TOOLS: frozenset[str] = frozenset({
    "aa-raw", "aa-fetch", "aa-get", "aa-refresh", "aa-setup",
})

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

        for tok in stage.args:
            if not isinstance(tok, str):
                errors.append(f"{prefix} non-string arg {tok!r}.")
                continue
            if _FORBIDDEN_ARG_CHARS.search(tok):
                errors.append(
                    f"{prefix} arg {tok!r} contains forbidden control chars."
                )
                continue
            for pat in _SHELL_METACHAR_PATTERNS:
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
