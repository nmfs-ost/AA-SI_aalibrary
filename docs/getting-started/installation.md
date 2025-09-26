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

## Step 1 - Installing the `gcloud` CLI Tool

To securely install AALibrary via pip, first we need to set up the Google Cloud CLI, `gcloud`:

=== "Windows"

    To set up `gcloud` on windows follow the instructions [here](https://cloud.google.com/sdk/docs/install#windows).

=== "Linux"

    Most GCP workstations, cloud shell editors, and compute engines come with `gcloud` preinstalled.
    
    If you have an instance without it installed, please follow the directions [here](https://cloud.google.com/sdk/docs/install#linux) to install it.

## Step 2 - Logging Into `gcloud`

Issue the following commands via Google Cloud SDK Shell (for Windows) or the terminal (for Linux). Follow the instructions to login to `gcloud` using your NOAA email. This authentication is necessary if you want to use `aalibrary` with its Google Cloud Platform capabilities.

```bash
gcloud auth login
gcloud auth application-default login
```

### Step 2.1 - Set Your Account As The Active Account For `gcloud`

```bash
gcloud config set account {ACCOUNT} 
```

Here, `{ACCOUNT}` should be your noaa.gov email. The same one you used to sign-in in the step above.

### Step 2.2 - Set The AA GCP Project As The Active Project For `gcloud`

```bash
gcloud config set project ggn-nmfs-aa-dev-1 
```

## Step 3 (Optional) - Install Virtual Environment Before The `pip install`

Some instances of Linux, such as GCP workstations, will not allow you to install packages. In this case, you will need to create a virtual environment to work out of.

=== "Windows"

    Please use your preferred package manager to create a virtual environment.

=== "Linux"

    If you would like to have a virtual environment to run out of, please use the following command:

    ```bash
    sudo apt-get update && sudo apt-get install python3-virtualenv -y
    python -m virtualenv my-venv
    ```

    !!! note "NOTE: Sudo:"
        Sudo is not required for these commands. You can try to run these commands with `sudo` removed if you do not have permissions.

## Step 4 - It's Finally `pip install` Time

To finally be able to pip-install the library use the following command:

If you do not have a virtual environment set up:

```bash

python -m pip install aalibrary@git+https://github.com/nmfs-ost/AA-SI_aalibrary.git
```

If you do have a virtual environment set up:

```bash

my-venv/bin/pip install aalibrary@git+https://github.com/nmfs-ost/AA-SI_aalibrary.git
```

!!! note "NOTE: If using virtual environments:"
    Since we have created a virtual environment, in order to use `aalibrary` simply replace all `python` commands with `{virtual env name}/bin/python` and all `pip` commands with `{virtual env name}/bin/pip`.

## Step 5 - Test it Out

Now that the library is installed, we can finally test it out. Open up a python  using the following command:

```bash
# If you do not have a virtual environment
python

# If you have a virtual environment
my-venv/bin/python
```

Next, we will enter the following code line-by-line. This will run a test function that will allow us to quickly test connectivity in our environment.

```python
from aalibrary import quick_test
quick_test.start()
```
