# set the correct logging here
import sys
import logging

# Import sub-packages
# flake8: noqa
from . import utils

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
