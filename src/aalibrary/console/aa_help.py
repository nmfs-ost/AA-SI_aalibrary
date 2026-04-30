"""aa-help: Vertex AI planner and explainer for the aalibrary toolkit.

Usage:
    aa-help                                  # interactive REPL (dry-run)
    aa-help "your question or goal"          # one-shot, dry-run by default
    aa-help --execute "..."                  # one-shot, allowed to run
    aa-help --setup                          # config wizard
    aa-help --edit                           # open config in $EDITOR
    aa-help --config                         # print config path
    aa-help --reindex                        # rebuild knowledge DB
    aa-help --refresh-index                  # incremental index update
    aa-help --index-stats                    # show what's indexed
    aa-help --model MODEL                    # override model for this run
"""

# === Silence logs BEFORE any heavy imports ===
import logging
import sys
import warnings

logging.disable(logging.CRITICAL)
warnings.filterwarnings("ignore")

from loguru import logger
logger.remove()
logger.add(sys.stderr, level="WARNING")

# Heavy imports
import argparse
from pathlib import Path

from aalibrary.utils._help import config as cfg
from aalibrary.utils._help import knowledge as kb
from aalibrary.utils._help.planner import Planner
from aalibrary.utils._help.ui import (
    handle_plan,
    print_banner,
    print_error,
    print_info,
    thinking,
)


def silence_all_logs():
    """Re-apply suppression after google-genai / httpx attach handlers lazily."""
    logging.disable(logging.CRITICAL)
    for name in [None] + list(logging.root.manager.loggerDict):
        lg = logging.getLogger(name)
        lg.handlers.clear()
        lg.propagate = True
    logger.remove()
    logger.add(sys.stderr, level="WARNING")


def _build_parser():
    p = argparse.ArgumentParser(
        prog="aa-help",
        description="Vertex AI planner & assistant for the aalibrary suite.",
    )
    p.add_argument("question", nargs="*",
                   help="One-shot question/goal. Omit to enter REPL mode.")
    p.add_argument("--execute", action="store_true",
                   help="Allow the planner to run pipelines (with confirm).")
    p.add_argument("--setup", action="store_true",
                   help="Run the configuration wizard.")
    p.add_argument("--config", action="store_true",
                   help="Print the config file path and exit.")
    p.add_argument("--edit", action="store_true",
                   help="Open the config file in $EDITOR.")
    p.add_argument("--reindex", action="store_true",
                   help="Rebuild the local knowledge DB from scratch.")
    p.add_argument("--refresh-index", action="store_true",
                   help="Incrementally update the knowledge DB.")
    p.add_argument("--index-stats", action="store_true",
                   help="Show how many files/chunks are indexed.")
    p.add_argument("--model",
                   help="Override the configured model for this run.")
    return p


def _do_index(rebuild: bool, settings: cfg.Settings) -> int:
    if not settings.knowledge_dirs:
        print_error("No `knowledge_dirs` configured. Edit your config first:")
        print_info(f"  {cfg.config_path()}")
        return 1
    print_info("Indexing knowledge dirs...")
    indexed, skipped, chunks = kb.build_or_refresh(
        [Path(d) for d in settings.knowledge_dirs],
        cfg.config_dir(),
        settings.project_id,
        settings.location,
        rebuild=rebuild,
    )
    print_info(f"Indexed: {indexed} files  ({chunks} new chunks)")
    print_info(f"Skipped: {skipped} files  (unchanged or empty)")
    s = kb.stats(cfg.config_dir())
    print_info(f"DB now contains: {s['files']} files, {s['chunks']} chunks "
               f"({s.get('size_bytes', 0):,} bytes)")
    return 0


def _do_index_stats() -> int:
    s = kb.stats(cfg.config_dir())
    print_info(f"DB path: {s['db_path']}")
    print_info(f"Files indexed: {s['files']}")
    print_info(f"Chunks indexed: {s['chunks']}")
    if "size_bytes" in s:
        print_info(f"DB size: {s['size_bytes']:,} bytes")
    return 0


def main(argv=None):
    args = _build_parser().parse_args(argv)

    # Cheap subcommands first -- no Vertex client needed.
    if args.config:
        print(cfg.config_path())
        return 0
    if args.edit:
        cfg.edit_config()
        return 0
    if args.setup:
        cfg.run_setup_wizard()
        return 0
    if args.index_stats:
        return _do_index_stats()

    settings = cfg.load_config()
    if not settings.is_complete():
        print_info("aa-help is not configured yet. Running setup...")
        settings = cfg.run_setup_wizard()
    if args.model:
        settings.model = args.model

    if args.reindex or args.refresh_index:
        return _do_index(rebuild=args.reindex, settings=settings)

    silence_all_logs()
    planner = Planner(settings)
    silence_all_logs()

    # One-shot or REPL?
    if args.question:
        question = " ".join(args.question)
        with thinking("planning"):
            plan = planner.plan(question)
        return handle_plan(plan, allow_execute=args.execute)

    # REPL
    print_banner(mode="execute" if args.execute else "dry-run")
    while True:
        try:
            line = input("\naa-help> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return 0
        if not line:
            continue
        if line in ("/exit", "/quit", ":q"):
            return 0
        try:
            with thinking("planning"):
                plan = planner.plan(line)
            handle_plan(plan, allow_execute=args.execute)
        except KeyboardInterrupt:
            print_info("[interrupted]")
        except Exception as e:
            print_error(str(e))


if __name__ == "__main__":
    raise SystemExit(main())