<!-- markdownlint-configure-file {
  "MD013": {
    "code_blocks": true,
    "tables": false
  },
  "MD033": false,
  "MD041": false,
  "MD013": false
} -->

# Installing `AALibrary`

???+ info "Google Cloud Workstations"
    If you are using Google Cloud Workstations, please follow the instructions outlined in the <a href="https://github.com/nmfs-ost/AA-SI_GCPSetup" target="_blank">repo here</a>.

## Dependencies

Dependencies are listed within `requirements.txt` and `setup.py`, however, they should be automatically installed when you first install `aalibrary`.

## Installation

To securely install this package via pip, use the following:

### Step 1 - Log Into `gcloud`

Issue the following command, and follow the instructions to login to `gcloud` using your NOAA email. This authentication is necessary if you want to use `aalibrary` with its Google Cloud Platform capabilities.

```bash
gcloud auth login
gcloud auth application-default login
```

### Step 1.1 - Set Your Account As The Active Account For `gcloud`

```bash
gcloud config set account {ACCOUNT} 
```

Here, `{ACCOUNT}` should be your noaa.gov email. The same one you used to sign-in in the step above.

### Step 1.2 - Set The AA GCP Project As The Active Project For `gcloud`

```bash
gcloud config set project ggn-nmfs-aa-dev-1 
```

### Step 2 - Install Necessary Dependencies Before The `pip install`

We need to install some dependencies, and check two authentication parameters before we install.

#### Step 2.1 - Run The Following Commands To Install Dependencies

```bash
sudo apt-get update && sudo apt-get install python3-virtualenv -y
python -m virtualenv my-venv
```

### Step 3 - It's Finally `pip install` Time

To finally be able to pip-install the library use the following command:

```bash
my-venv/bin/pip install aalibrary@git+https://github.com/nmfs-ost/AA-SI_aalibrary.git
```

!!! note "NOTE"
    Since we have created a virtual environment, in order to use `aalibrary` simply replace all `python` commands with `my-venv/bin/python` and all `pip` commands with `my-venv/bin/pip`.

### Step 4 - Test it Out

Now that the library is installed, we can finally test it out. Open up a python  using the following command:

```bash
my-venv/bin/python
```

Next, we will enter the following code line-by-line. This will run a test function that will allow us to quickly test connectivity in our environment.

```python
from aalibrary import quick_test
quick_test.start()
```
