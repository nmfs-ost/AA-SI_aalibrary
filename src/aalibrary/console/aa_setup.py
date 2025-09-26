import subprocess
import sys

def print_help():
    help_text = """
    Usage: aa-setup

    Description:
    Reinstalls the startup script for the AA-SI GPCSetup environment on a Google Cloud VM. 

    """
    print(help_text)
    

def main():
    
    if '--help' in sys.argv or '-h' in sys.argv:
        print_help()
        return

    cmd = f"""
    cd ~ && \
    sudo rm -f init.sh && \
    sudo wget https://raw.githubusercontent.com/nmfs-ost/AA-SI_GPCSetup/main/init.sh && \
    sudo chmod +x init.sh && \
    ./init.sh && \
    cd ~ && \
    source venv3.12/bin/activate && \
    gcloud auth application-default login && \
    gcloud config set account {{ACCOUNT}} && \
    gcloud config set project ggn-nmfs-aa-dev-1
    """

    # Run the full shell command
    subprocess.run(cmd, shell=True, executable="/bin/bash")


if __name__ == "__main__":
    main()
