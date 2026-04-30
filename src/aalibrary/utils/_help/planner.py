"""Planner client. Calls Vertex Gemini with the system prompt + retrieved
RAG chunks + the user's question, and parses the JSON Plan it returns.

Why JSON-only output (not streaming)?
  The planner is a structured-output call -- we need a complete object before
  we can safely show menus or run anything. Streaming a half-parsed JSON
  blob to a TUI is a bad time. The free-form "answer" branch still gets
  rendered as markdown by the UI, but it arrives in one shot.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Optional

from .config import Settings
from . import knowledge as kb
from .plan import Plan, PLAN_SCHEMA_DESCRIPTION


_INSTALL_HINT = (
    "Missing dependency: install with `pip install google-genai`.\n"
    "Then authenticate once with: `gcloud auth application-default login`"
)


def _load_base_prompt() -> str:
    """Read the static system.md from the package."""
    from importlib import resources
    try:
        return (resources.files(__package__)
                .joinpath("prompts/system.md")
                .read_text(encoding="utf-8"))
    except Exception:
        return ("You are aa-help, an assistant for the aalibrary "
                "active-acoustics toolkit.")


def _format_rag_block(hits: list[kb.Hit], max_chars: int) -> str:
    if not hits:
        return ""
    parts: list[str] = ["\n\n=== RETRIEVED KNOWLEDGE ===\n"]
    total = 0
    for h in hits:
        snippet = (
            f"\n--- {h.path} (chunk {h.chunk_index}, score {h.score:.3f}) ---\n"
            f"{h.text}\n"
        )
        if total + len(snippet) > max_chars:
            parts.append(f"\n[...truncated; max_chars={max_chars}]\n")
            break
        parts.append(snippet)
        total += len(snippet)
    return "".join(parts)


def _extract_json(text: str) -> Optional[dict]:
    """Pull the first JSON object out of the model's reply.

    Gemini occasionally wraps JSON in ```json fences despite instructions, so
    we strip those and then find the first balanced {...}. If parsing fails
    we return None and the caller falls back to treating the reply as a
    plain answer.
    """
    text = text.strip()
    # Strip code fences if present.
    fence = re.match(r"^```(?:json)?\s*(.*?)\s*```\s*$", text, re.DOTALL)
    if fence:
        text = fence.group(1)
    # Find the first {, then walk to its matching }.
    start = text.find("{")
    if start == -1:
        return None
    depth = 0
    in_str = False
    esc = False
    for i in range(start, len(text)):
        c = text[i]
        if in_str:
            if esc:
                esc = False
            elif c == "\\":
                esc = True
            elif c == '"':
                in_str = False
            continue
        if c == '"':
            in_str = True
        elif c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                blob = text[start:i + 1]
                try:
                    return json.loads(blob)
                except json.JSONDecodeError:
                    return None
    return None


class Planner:
    def __init__(self, settings: Settings):
        try:
            from google import genai
            from google.genai import types
        except ModuleNotFoundError as e:
            raise SystemExit(f"{_INSTALL_HINT}\n(import error: {e})")
        self._genai = genai
        self._types = types
        self._settings = settings
        try:
            self._client = genai.Client(
                vertexai=True,
                project=settings.project_id,
                location=settings.location,
            )
        except Exception as e:
            raise SystemExit(
                f"Failed to initialize Vertex AI client: {e}\n"
                "Check `project_id` and `location` in your config, and that "
                "you've run `gcloud auth application-default login`."
            )
        self._base_prompt = _load_base_prompt()

    def _build_system_prompt(self, question: str) -> str:
        sections: list[str] = [self._base_prompt]

        # Retrieve from local knowledge base if it exists.
        try:
            from .config import config_dir
            hits = kb.search(
                question,
                config_dir(),
                self._settings.project_id,
                self._settings.location,
                top_k=self._settings.rag_top_k,
            )
        except Exception as e:
            sys.stderr.write(f"aa-help: RAG lookup failed: {e}\n")
            hits = []
        if hits:
            sections.append(_format_rag_block(hits, self._settings.rag_max_chars))

        if self._settings.extra_system_prompt.strip():
            sections.append("\n\n=== USER NOTES ===\n"
                            + self._settings.extra_system_prompt.strip())

        sections.append("\n\n=== OUTPUT FORMAT ===\n" + PLAN_SCHEMA_DESCRIPTION)
        return "".join(sections)

    def plan(self, question: str) -> Plan:
        types = self._types
        system_prompt = self._build_system_prompt(question)
        config = types.GenerateContentConfig(
            system_instruction=system_prompt,
            temperature=self._settings.temperature,
            max_output_tokens=self._settings.max_output_tokens,
            response_mime_type="application/json",
        )
        try:
            resp = self._client.models.generate_content(
                model=self._settings.model,
                contents=[types.Content(role="user",
                                        parts=[types.Part(text=question)])],
                config=config,
            )
        except Exception as e:
            return Plan(kind="answer",
                        answer=f"Vertex API call failed: {e}")

        raw = resp.text or ""
        data = _extract_json(raw)
        if data is None:
            # Model ignored the schema; treat as a plain answer.
            return Plan(kind="answer", answer=raw.strip()
                        or "(empty response)")
        try:
            return Plan.from_dict(data)
        except Exception as e:
            return Plan(kind="answer",
                        answer=f"Plan parse error ({e}). Raw response:\n{raw}")
