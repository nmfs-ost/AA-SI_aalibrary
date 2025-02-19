# Testing for install on Linux-based systems.
# Creates and uses a python virtual environment.
sudo apt-get update && sudo apt-get install python3-virtualenv -y

python -m virtualenv my-venv

my-venv/bin/pip install aalibrary@git+https://github.com/nmfs-ost/AA-SI_aalibrary.git
