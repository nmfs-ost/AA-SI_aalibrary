"""Plan schema -- the contract between the Gemini planner and the local executor.

Gemini returns one of these shapes as JSON. Anything else is a planner error
and gets shown as a plain text message instead of a runnable plan.
"""
from __future__ import annotations

from dataclasses import dataclass, field


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
    risks: list[str] = field(default_factory=list)
    answer: str = ""                   # populated when kind == "answer"
    question: str = ""                 # populated when kind == "clarify"
    options: list[str] = field(default_factory=list)   # menu choices for "clarify"

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
            options=list(d.get("options", [])),
        )


# JSON schema description we paste into the system prompt so Gemini emits
# matching shapes. Keep this string in sync with `from_dict` above.
PLAN_SCHEMA_DESCRIPTION = """\
Respond with a single JSON object. No markdown fences, no prose outside the JSON.

The object has one required field, `kind`, which is one of:
  "pipeline" -- you have a runnable aa-* pipeline to propose
  "answer"   -- the user asked a knowledge question, no command to run
  "clarify"  -- you genuinely need one piece of info before you can plan

PREFER PIPELINE OR ANSWER. Clarification is a last resort: it costs the user
a round-trip. Before returning kind="clarify", ask yourself: can I make a
reasonable default and proceed? If yes, do that and surface the assumption
as a `risk` ("assumed EK60 -- change with --sonar_model EK80 if wrong").

Schema:
{
  "kind": "pipeline" | "answer" | "clarify",

  // kind="pipeline":
  "summary": "one-paragraph plain English of what the pipeline does",
  "stages": [
    {
      "tool": "aa-sv",
      "args": ["--waveform_mode", "BB"],
      "explanation": "one short line"
    }
  ],
  "expected_output": "./cruise_Sv_clean_mvbs.nc",
  "risks": ["downloads ~2GB from Azure", "assumed EK60 sonar model"],

  // kind="answer":
  "answer": "free-form markdown text",

  // kind="clarify":
  "question": "the single most blocking question",
  "options": [
    "EK60 (legacy narrowband)",
    "EK80 (modern, supports BB)",
    "ME70 (multibeam)"
  ]
}

Rules for "clarify":
- ALWAYS include `options` -- 2 to 4 plausible answers. Never ask without them.
- Each option is a SHORT phrase, not a full sentence. The user picks one with
  arrow keys; long options are awkward to scan.
- If you literally cannot enumerate plausible answers (rare), use kind="answer"
  instead and explain what info you need in plain prose.

Rules for "pipeline":
- Every `tool` MUST be a real aa-* command from the reference cards. Never invent.
- `args` is a list of argv tokens. Do NOT include the tool name. Do NOT include
  pipe characters, redirects, env vars, or shell substitutions.
- If a flag takes a value, split it: ["--sonar_model", "EK60"], not
  ["--sonar_model=EK60"].
- If a default is reasonable, use it and note the assumption in `risks`.

Rules for "answer":
- For knowledge questions (no run intent), return free-form markdown.
- Be concise. Lead with the answer.
"""