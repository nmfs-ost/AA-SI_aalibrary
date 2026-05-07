"""Local RAG knowledge store.

Indexes a directory tree of supported files into a SQLite database alongside
their Vertex-computed embeddings. At query time, embeds the question and
returns the top-K most similar chunks for context injection.

Why local SQLite + numpy and not a vector DB?
  - Your corpus is "large" but bounded (manuals + source + a few hundred PDFs).
    Brute-force cosine over a few thousand vectors takes <50ms on a laptop.
  - Zero infrastructure. The .db file lives next to your config and is the
    full source of truth; delete it to re-index.
  - Drop-in upgrade path later: swap _search() for FAISS or a managed service
    when you actually need it. The rest of the module is untouched.

Supported inputs:
  .md .txt .rst .py .toml .json .yaml .yml   -- read as utf-8 text
  .pdf                                       -- text extracted via pypdf
  .ipynb                                     -- code+markdown cells extracted

Index lifecycle:
  - Files are hashed (mtime + size). Unchanged files are skipped on reindex.
  - Run `aa-help --reindex` to rebuild from scratch.
  - Run `aa-help --refresh-index` (alias of build_or_refresh) to incrementally
    update only changed files.
"""
from __future__ import annotations

import hashlib
import json
import math
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional


# -- file readers ------------------------------------------------------------

_TEXT_EXTS = {".md", ".txt", ".rst", ".py", ".toml", ".json", ".yaml", ".yml"}


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


def _read_pdf(path: Path) -> str:
    try:
        from pypdf import PdfReader
    except ModuleNotFoundError:
        sys.stderr.write(
            f"aa-help: skipping {path.name}: install `pypdf` to index PDFs.\n"
        )
        return ""
    try:
        reader = PdfReader(str(path))
    except Exception as e:
        sys.stderr.write(f"aa-help: failed to open {path}: {e}\n")
        return ""
    parts: list[str] = []
    for i, page in enumerate(reader.pages):
        try:
            parts.append(f"\n[page {i + 1}]\n{page.extract_text() or ''}")
        except Exception:
            continue
    return "".join(parts)


def _read_ipynb(path: Path) -> str:
    try:
        nb = json.loads(path.read_text(encoding="utf-8", errors="replace"))
    except Exception:
        return ""
    out: list[str] = []
    for cell in nb.get("cells", []):
        kind = cell.get("cell_type", "")
        src = cell.get("source", "")
        if isinstance(src, list):
            src = "".join(src)
        if kind == "code":
            out.append(f"\n```python\n{src}\n```\n")
        elif kind == "markdown":
            out.append(f"\n{src}\n")
    return "".join(out)


def _read_file(path: Path) -> str:
    suf = path.suffix.lower()
    if suf in _TEXT_EXTS:
        return _read_text(path)
    if suf == ".pdf":
        return _read_pdf(path)
    if suf == ".ipynb":
        return _read_ipynb(path)
    return ""


def _is_indexable(path: Path) -> bool:
    if not path.is_file():
        return False
    suf = path.suffix.lower()
    return suf in _TEXT_EXTS or suf in {".pdf", ".ipynb"}


# -- chunking ----------------------------------------------------------------

# Roughly 1500 chars per chunk with 200 char overlap. Smaller than typical
# "embed everything" RAG so source code chunks stay coherent.
CHUNK_SIZE = 1500
CHUNK_OVERLAP = 200


def _chunk(text: str) -> list[str]:
    text = text.strip()
    if not text:
        return []
    if len(text) <= CHUNK_SIZE:
        return [text]
    chunks: list[str] = []
    i = 0
    n = len(text)
    while i < n:
        j = min(i + CHUNK_SIZE, n)
        # Try to break on a paragraph or sentence boundary near j.
        if j < n:
            for sep in ("\n\n", "\n", ". "):
                k = text.rfind(sep, i + CHUNK_SIZE // 2, j)
                if k != -1:
                    j = k + len(sep)
                    break
        chunks.append(text[i:j].strip())
        if j >= n:
            break
        i = max(j - CHUNK_OVERLAP, i + 1)
    return [c for c in chunks if c]


# -- store -------------------------------------------------------------------

@dataclass
class Hit:
    path: str
    chunk_index: int
    text: str
    score: float


_SCHEMA = """
CREATE TABLE IF NOT EXISTS files (
    path TEXT PRIMARY KEY,
    fingerprint TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS chunks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    path TEXT NOT NULL,
    chunk_index INTEGER NOT NULL,
    text TEXT NOT NULL,
    embedding BLOB NOT NULL,
    FOREIGN KEY (path) REFERENCES files(path) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_chunks_path ON chunks(path);
"""


def _fingerprint(p: Path) -> str:
    st = p.stat()
    return f"{st.st_mtime_ns}:{st.st_size}"


def _open_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.executescript(_SCHEMA)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


# -- embeddings (Vertex via google-genai) ------------------------------------

EMBED_MODEL = "text-embedding-005"  # Vertex AI text embedding


def _embed_batch(texts: list[str], project_id: str, location: str) -> list[list[float]]:
    """Embed a batch of texts via Vertex AI. Returns one vector per input."""
    try:
        from google import genai
        from google.genai import types
    except ModuleNotFoundError as e:
        raise SystemExit(
            f"aa-help: missing google-genai. {e}\n"
            "Install with `pip install google-genai`."
        )
    client = genai.Client(vertexai=True, project=project_id, location=location)
    out: list[list[float]] = []
    # Vertex embed API caps batch sizes; chunk into 250s to stay safe.
    for i in range(0, len(texts), 250):
        batch = texts[i:i + 250]
        resp = client.models.embed_content(
            model=EMBED_MODEL,
            contents=batch,
            config=types.EmbedContentConfig(task_type="RETRIEVAL_DOCUMENT"),
        )
        for emb in resp.embeddings:
            out.append(list(emb.values))
    return out


def _embed_query(text: str, project_id: str, location: str) -> list[float]:
    from google import genai
    from google.genai import types
    client = genai.Client(vertexai=True, project=project_id, location=location)
    resp = client.models.embed_content(
        model=EMBED_MODEL,
        contents=[text],
        config=types.EmbedContentConfig(task_type="RETRIEVAL_QUERY"),
    )
    return list(resp.embeddings[0].values)


def _vec_to_blob(v: list[float]) -> bytes:
    import struct
    return struct.pack(f"{len(v)}f", *v)


def _blob_to_vec(b: bytes) -> list[float]:
    import struct
    return list(struct.unpack(f"{len(b) // 4}f", b))


def _cosine(a: list[float], b: list[float]) -> float:
    s = 0.0
    na = 0.0
    nb = 0.0
    for x, y in zip(a, b):
        s += x * y
        na += x * x
        nb += y * y
    if na == 0 or nb == 0:
        return 0.0
    return s / math.sqrt(na * nb)


# -- public API --------------------------------------------------------------

def db_path(config_dir: Path) -> Path:
    return config_dir / "knowledge.db"


def build_or_refresh(
    knowledge_dirs: list[Path],
    config_dir: Path,
    project_id: str,
    location: str,
    *,
    rebuild: bool = False,
) -> tuple[int, int, int]:
    """Walk the dirs and (re)index files whose fingerprints changed.

    Returns (files_indexed, files_skipped, chunks_added).
    """
    path = db_path(config_dir)
    if rebuild and path.exists():
        path.unlink()
    conn = _open_db(path)
    cur = conn.cursor()

    indexed = 0
    skipped = 0
    chunks_added = 0

    for root in knowledge_dirs:
        root = root.expanduser()
        if not root.exists():
            sys.stderr.write(f"aa-help: knowledge dir not found: {root}\n")
            continue
        files = sorted(root.rglob("*")) if root.is_dir() else [root]
        for f in files:
            if not _is_indexable(f):
                continue
            fp = _fingerprint(f)
            row = cur.execute(
                "SELECT fingerprint FROM files WHERE path = ?", (str(f),)
            ).fetchone()
            if row and row[0] == fp:
                skipped += 1
                continue

            text = _read_file(f)
            chunks = _chunk(text)
            if not chunks:
                skipped += 1
                continue

            sys.stderr.write(f"  indexing {f}  ({len(chunks)} chunks)\n")
            sys.stderr.flush()

            try:
                vecs = _embed_batch(chunks, project_id, location)
            except Exception as e:
                sys.stderr.write(f"  failed to embed {f}: {e}\n")
                continue

            cur.execute("DELETE FROM files WHERE path = ?", (str(f),))
            cur.execute("INSERT INTO files(path, fingerprint) VALUES (?, ?)",
                        (str(f), fp))
            for i, (chunk_text, vec) in enumerate(zip(chunks, vecs)):
                cur.execute(
                    "INSERT INTO chunks(path, chunk_index, text, embedding) "
                    "VALUES (?, ?, ?, ?)",
                    (str(f), i, chunk_text, _vec_to_blob(vec)),
                )
                chunks_added += 1
            conn.commit()
            indexed += 1

    conn.close()
    return indexed, skipped, chunks_added


def search(
    query: str,
    config_dir: Path,
    project_id: str,
    location: str,
    *,
    top_k: int = 8,
) -> list[Hit]:
    """Embed query, return top-K matching chunks by cosine similarity."""
    path = db_path(config_dir)
    if not path.exists():
        return []
    qvec = _embed_query(query, project_id, location)
    conn = _open_db(path)
    cur = conn.cursor()
    rows = cur.execute(
        "SELECT path, chunk_index, text, embedding FROM chunks"
    ).fetchall()
    conn.close()
    scored: list[Hit] = []
    for p, idx, text, blob in rows:
        v = _blob_to_vec(blob)
        score = _cosine(qvec, v)
        scored.append(Hit(path=p, chunk_index=idx, text=text, score=score))
    scored.sort(key=lambda h: h.score, reverse=True)
    return scored[:top_k]


def stats(config_dir: Path) -> dict:
    """Quick summary of what's indexed -- used by `aa-help --index-stats`."""
    path = db_path(config_dir)
    if not path.exists():
        return {"files": 0, "chunks": 0, "db_path": str(path)}
    conn = _open_db(path)
    cur = conn.cursor()
    n_files = cur.execute("SELECT COUNT(*) FROM files").fetchone()[0]
    n_chunks = cur.execute("SELECT COUNT(*) FROM chunks").fetchone()[0]
    conn.close()
    return {
        "files": n_files,
        "chunks": n_chunks,
        "db_path": str(path),
        "size_bytes": path.stat().st_size,
    }
