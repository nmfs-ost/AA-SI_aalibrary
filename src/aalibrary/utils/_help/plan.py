"""Plan schema -- the contract between the Gemini planner and the local executor.

Gemini returns one of these shapes as JSON. Anything else is a planner error
and gets shown as a plain text message instead of a runnable plan.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class PipelineStage:
    """One `aa-*` invocation in a pipeline."""
    tool: str                          # e.g. "aa-sv"
    args: list[str] = field(default_factory=list)   # e.g. ["--waveform_mode", "BB"]
    explanation: str = ""              # one short line: what this stage does


@dataclass
class Plan:
    """A proposed pipeline + supporting context."""
    kind: str                          # "pipeline" | "answer" | "clarify"
    summary: str = ""                  # one-paragraph plain-English summary
    stages: list[PipelineStage] = field(default_factory=list)
    expected_output: str = ""          # e.g. "./cruise_Sv_clean_mvbs.nc"
    risks: list[str] = field(default_factory=list)  # e.g. "downloads ~2GB"
    answer: str = ""                   # populated when kind == "answer"
    question: str = ""                 # populated when kind == "clarify"

    @classmethod
    def from_dict(cls, d: dict) -> "Plan":
        kind = d.get("kind", "answer")
        stages = [
            PipelineStage(
                tool=s.get("tool", ""),
                args=list(s.get("args", [])),
                explanation=s.get("explanation", ""),
            )
            for s in d.get("stages", [])
        ]
        return cls(
            kind=kind,
            summary=d.get("summary", ""),
            stages=stages,
            expected_output=d.get("expected_output", ""),
            risks=list(d.get("risks", [])),
            answer=d.get("answer", ""),
            question=d.get("question", ""),
        )


# JSON schema description we paste into the system prompt so Gemini emits
# matching shapes. Keep this string in sync with `from_dict` above.
PLAN_SCHEMA_DESCRIPTION = """\
Respond with a single JSON object. No markdown fences, no prose outside the JSON.

The object has one required field, `kind`, which is one of:
  "pipeline" -- you have a runnable aa-* pipeline to propose
  "answer"   -- the user asked a knowledge question, no command to run
  "clarify"  -- you need one piece of info before you can plan

Schema:
{
  "kind": "pipeline" | "answer" | "clarify",

  // For kind="pipeline":
  "summary": "one-paragraph plain English of what the pipeline does",
  "stages": [
    {
      "tool": "aa-sv",                       // must be a real aa-* tool name
      "args": ["--waveform_mode", "BB"],     // argv list, no shell metachars
      "explanation": "one short line"
    },
    ...
  ],
  "expected_output": "./cruise_Sv_clean_mvbs.nc",
  "risks": ["downloads ~2GB from Azure", "overwrites cruise.nc"],

  // For kind="answer":
  "answer": "free-form markdown text",

  // For kind="clarify":
  "question": "the single most blocking question"
}

Rules:
- Every `tool` MUST be a real aa-* command from the reference cards. Never invent.
- `args` is a list of argv tokens. Do NOT include the tool name. Do NOT include
  pipe characters, redirects, env vars, or shell substitutions. The runner
  wires up piping itself.
- If a flag takes a value, split it: ["--sonar_model", "EK60"], not
  ["--sonar_model=EK60"].
- If you don't know enough to plan, return kind="clarify" with one question.
- If the user asked a knowledge question (no run intent), return kind="answer".
"""
