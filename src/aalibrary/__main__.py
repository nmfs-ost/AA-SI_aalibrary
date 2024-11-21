import argparse
import quick_test


parser = argparse.ArgumentParser(
    description="AA LIBRARY", formatter_class=argparse.ArgumentDefaultsHelpFormatter
)
parser.add_argument(
    "-t",
    "--test",
    action="store_true",
    help="Flag for whether you want to run the basic tests to see whether the environment is set up correctly..",
)

args = parser.parse_args()

if args.test:
    # run quick_test here.
    quick_test.quick_test()