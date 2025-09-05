from aalibrary import quick_test
import sys
import subprocess


def console_run(command, capture_output=True, shell=False):
    """
    Run a shell command from Python.

    Args:
        command (str or list): Command to run.
        capture_output (bool): If True, capture stdout and stderr.
        shell (bool): If True, run command through the shell.

    Returns:
        result (subprocess.CompletedProcess): Contains stdout, stderr, returncode.
    """
    if capture_output:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,  # returns output as string
            shell=shell,
            check=False,  # don't raise exception on non-zero exit
        )
    else:
        result = subprocess.run(command, shell=shell)
    return result


def main():

    # Example usage:
    cmd = "aa-nc /home/mryan/Desktop/HB1603_L1-D20160707-T190150.raw --sonar_model EK60 | aa-clean --ping_num 20 --range_sample_num 20"
    result = console_run(cmd, shell=True)
    print("Return code:", result.returncode)
    print("STDOUT:\n", result.stdout)
    print("STDERR:\n", result.stderr)
    sys.exit(0)


if __name__ == "__main__":
    main()
