import subprocess


def main():

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
