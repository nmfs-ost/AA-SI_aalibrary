"""aa-help: Vertex AI assistant for the aalibrary active-acoustics toolkit.

Usage:
    aa-help                      # interactive REPL
    aa-help "your question"      # one-shot answer
    aa-help --setup              # configuration wizard
    aa-help --edit               # open config in $EDITOR
    aa-help --config             # print config path
    aa-help --show-context       # print the assembled system prompt (debug)
    aa-help --no-stream          # disable streaming
    aa-help --model MODEL        # override model for this run
"""

# === Silence logs BEFORE any heavy imports (matches aa-sv pattern) ===
import logging
import sys
import warnings

logging.disable(logging.CRITICAL)
warnings.filterwarnings("ignore")

from loguru import logger
logger.remove()
# WARNING+ to stderr so real errors aren't swallowed.
logger.add(sys.stderr, level="WARNING")

# Now the heavy imports - anything they log gets squashed
import argparse

from aalibrary.utils._help import config as cfg
from aalibrary.utils._help import context as ctx
from aalibrary.utils._help.repl import run_repl
from aalibrary.utils._help.vertex_client import VertexHelper


def silence_all_logs():
    """Re-apply suppression in case google-genai / httpx / grpc re-enabled
    logging or added their own handlers during initialization."""
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
        description="Active Acoustics assistant powered by Vertex AI Gemini.",
    )
    p.add_argument(
        "question",
        nargs="*",
        help="Ask a one-shot question. If omitted, drops into interactive mode.",
    )
    p.add_argument("--setup", action="store_true",
                   help="Run the configuration wizard.")
    p.add_argument("--config", action="store_true",
                   help="Print the config file path and exit.")
    p.add_argument("--edit", action="store_true",
                   help="Open the config file in $EDITOR.")
    p.add_argument("--show-context", action="store_true",
                   help="Print the assembled system prompt and exit (debug).")
    p.add_argument("--model",
                   help="Override the configured model for this run.")
    p.add_argument("--no-stream", action="store_true",
                   help="Disable response streaming.")
    return p


def main(argv=None):
    args = _build_parser().parse_args(argv)

    if args.config:
        print(cfg.config_path())
        return 0

    if args.edit:
        cfg.edit_config()
        return 0

    if args.setup:
        cfg.run_setup_wizard()
        return 0

    settings = cfg.load_config()
    if not settings.is_complete():
        logger.warning("aa-help is not configured yet. Running setup wizard...")
        settings = cfg.run_setup_wizard()

    if args.model:
        settings.model = args.model

    system_prompt = ctx.build_system_prompt(settings)

    if args.show_context:
        print(system_prompt)
        return 0

    # Re-silence right before instantiating the Vertex client - google-auth
    # and httpx attach handlers during first use, not at import time.
    silence_all_logs()
    helper = VertexHelper(settings, system_prompt=system_prompt)
    silence_all_logs()

    if args.question:
        question = " ".join(args.question)
        helper.ask(question, stream=not args.no_stream)
        return 0

    return run_repl(helper)


if __name__ == "__main__":
    raise SystemExit(main())