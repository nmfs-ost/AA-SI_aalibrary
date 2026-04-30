"""Thin wrapper around google-genai for Vertex AI Gemini calls.

Uses Application Default Credentials. Run `gcloud auth application-default login`
once on your workstation and this module just works -- no API keys in code.
"""
from __future__ import annotations

import sys

from .config import Settings


_INSTALL_HINT = (
    "Missing dependency: install with `pip install google-genai`.\n"
    "Then authenticate once with: `gcloud auth application-default login`"
)


class VertexHelper:
    def __init__(self, settings: Settings, system_prompt: str):
        try:
            from google import genai
            from google.genai import types
        except ModuleNotFoundError as e:
            raise SystemExit(f"{_INSTALL_HINT}\n(import error: {e})")
        self._genai = genai
        self._types = types
        self._settings = settings
        self._system_prompt = system_prompt
        try:
            self._client = genai.Client(
                vertexai=True,
                project=settings.project_id,
                location=settings.location,
            )
        except Exception as e:  # noqa: BLE001
            raise SystemExit(
                f"Failed to initialize Vertex AI client: {e}\n"
                "Check `project_id` and `location` in your config, and that you've run\n"
                "`gcloud auth application-default login`."
            )
        self._history: list = []  # list[types.Content]

    def _gen_config(self):
        return self._types.GenerateContentConfig(
            system_instruction=self._system_prompt,
            temperature=self._settings.temperature,
            max_output_tokens=self._settings.max_output_tokens,
        )

    def ask(self, question: str, stream: bool = True) -> str:
        types = self._types
        user_msg = types.Content(role="user", parts=[types.Part(text=question)])
        contents = self._history + [user_msg]

        if stream:
            chunks: list[str] = []
            try:
                for ev in self._client.models.generate_content_stream(
                    model=self._settings.model,
                    contents=contents,
                    config=self._gen_config(),
                ):
                    text = getattr(ev, "text", None)
                    if text:
                        sys.stdout.write(text)
                        sys.stdout.flush()
                        chunks.append(text)
                sys.stdout.write("\n")
                sys.stdout.flush()
            except KeyboardInterrupt:
                sys.stdout.write("\n[interrupted]\n")
                sys.stdout.flush()
            answer = "".join(chunks)
        else:
            resp = self._client.models.generate_content(
                model=self._settings.model,
                contents=contents,
                config=self._gen_config(),
            )
            answer = resp.text or ""
            print(answer)

        if answer:
            self._history.append(user_msg)
            self._history.append(
                types.Content(role="model", parts=[types.Part(text=answer)])
            )
        return answer

    def reset(self) -> None:
        self._history.clear()