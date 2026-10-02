# One-Page Overview for AALibrary

This one-pager document covers all that is necessary to get started with AALibrary and the console tools.

## 1. Permissions

Each portion/project of the AASI requires separate permissions. You can find instructions for obtaining permissions to the various aspects of AALibrary on the [permissions page](../getting-started/permissions.md).

## 2. Installation

The installation of AALibrary varies by the operating system. The console tools come bundled with AALibrary, and are accessible as command line commands. You can find more instructions on how to install AALibrary on the [Installation](../getting-started/installation.md) page.

## 3. Data Life Cycle

The data life cycle for AASI is pretty straight-forward, as it is based on one main goal, the archival of water column sonar data. Archived data can be retrieved for analysis later, so this takes priority over just cloud-upload and analysis.

### Archival Life Cycle

Data Collection from Ships >> Data Analysis on the Cloud >> Data Archival to NCEI

#### Data Collection From Ships

Firstly, data is collected from ships using echosounders and various other instruments such as temperature sensors. This same data is then stored on the ships computer. Previously, the hard drive from this computer had to be removed, and copied over to the respective science center. However, AALibrary helps to mitigate that by providing functions to users to upload their files from the ship directly. Data can be uploaded using AALibrary Python code, Console Tools, or the web browser for Google Cloud Storage.

#### Data Analysis On Local/Cloud

For analysis, we utilize four different data level categories. More information on these can be found on the [DataRoadMap page](https://github.com/nmfs-ost/AA-SI_DataRoadMap).

The majority of the analysis functionality provided by AALibrary is found in the console tools, which have their own [documentation page](../documentation/console_tools.md). Console tools are provided through the command line, and can be piped together to create mini-pipelines for exploratory data analysis.

There is a more dedicated pipeline tool being worked on by AASI named the [Recipe Manager](https://github.com/BLayman-NOAA/AA-SI_recipe_manager). The Recipe Manager allows for users to conduct exploratory data analysis and model creation. The benefit of this product is that it allows users to capture pipeline metadata through "recipe files" in order to achieve reproducibility.

There is an external product also being actively developed by echostack named [echodataflow](https://github.com/echostack-org/echodataflow). However, this product is currently better suited for immediate data processing and analysis (i.e. to achieve near-real-time analysis).

#### Data Archival To NCEI

The overall goal of AASI is to get data up into the cloud. This is done by uploading data to GCP, generating the correct calibration metadata and such, and then archiving the data in NCEI.

The data we have archived so far is publicly available, and can be seen on the [NCEI S3 Bucket](https://noaa-wcsd-pds.s3.amazonaws.com/index.html#data/raw/).

The tool used to archive GCP data to NCEI is called Tugboat. Tugboat requires a JSON file filled with survey metadata. Tugboat then pulls the data from GCP Storage Buckets.More information can be found on the [Tugboat Integration](../usage/tugboat.md) page.

#### Overall Architecture

More details on the data lifecycle architecture implementation can be found through [this diagram](https://drive.google.com/file/d/1kleBzjKXvX-mJejetF4qljxkSFSp18Uy/view?usp=sharing).

## 4. Data Storage Standards In GCP

Data storage standards in GCP are outlined in the [GCP Overview](./gcp_overview.md) documentation page. The standards cover directory layouts, file naming conventions, the metadata database, derived products, and CruisePack historical data.

GCP storage bucket locations are parsed based on need. Whether it be to find the correct location based on our directory layout, or the file type. You can find functions for these located in the [`helpers.py`](https://github.com/nmfs-ost/AA-SI_aalibrary/blob/main/src/aalibrary/utils/helpers.py) module.

Furthermore, ship names in AALibrary are usually normalized before uploads are committed. The function to standardize ship names can be found in the [`helpers.py`](https://github.com/nmfs-ost/AA-SI_aalibrary/blob/main/src/aalibrary/utils/helpers.py) module. You can also normalize ship names based on ICES conventions, by utilizing the [functions found in AALibrary](https://github.com/nmfs-ost/AA-SI_aalibrary/blob/main/src/aalibrary/utils/ices.py).

## 5. Metadata DB

The metadata database is a [database within BigQuery](https://console.cloud.google.com/bigquery?hl=en&invt=AbuwBQ&project=ggn-nmfs-aa-prod-1&ws=!1m5!1m4!3m2!1sggn-nmfs-aa-prod-1!2smetadata!23sRESOURCE_LIST) used for storing various metadata about files that have been uploaded to Google Cloud Storage. It is a constant work-in-progress with updates to columns, and table additions nearly every month, based on the needs of the AALibrary developers. Most of the tables contain detailed information about file-level metadata, or survey metadata.

There were further plans to incorporate metadata gathered from Coriolix using an [API](https://github.com/nmfs-ost/AA-SI_aalibrary/blob/main/other/coriolix_api.py). This was stopped, as ships that collect water column sonar data (WCSD) were not uploading their metadata to Coriolix at the time. There were also other plans on incorporating data gathered from the [NCEI WCSD viewer API](https://gis.ngdc.noaa.gov/arcgis/rest/services/wcsd_files/MapServer/1/query?f=pjson&where=CLOUD_PATH+IS+NOT+NULL+AND+DATASET_NAME+in+%28%27DY2207_EK80%27%29+AND+%28UPPER%28INSTRUMENT_NAME%29+LIKE+%27%25EK80%25%27+AND+%28UPPER%28FREQUENCY%29+LIKE+%27%2518WKHZ%25%27+OR+UPPER%28FREQUENCY%29+LIKE+%27%2538WKHZ%25%27+OR+UPPER%28FREQUENCY%29+LIKE+%27%2570WKHZ%25%27+OR+UPPER%28FREQUENCY%29+LIKE+%27%25120WKHZ%25%27+OR+UPPER%28FREQUENCY%29+LIKE+%27%25200WKHZ%25%27%29+AND+CAL_STATE_VALUE+IN+%284%29%29&outFields=FILE_NAME%2CCLOUD_PATH%2CUSE_RECURSIVE&returnGeometry=false&orderByFields=FILE_NAME), however the API has not been implemented in AALibrary yet. Lastly, there were plans on incorporating InPort metadata into the archiving pipeline (fetching data from the metadata database to automatically create an InPort DMP), however, it has not been achieved as of yet. There is, however, [code for validating an InPort DMP](https://github.com/nmfs-ost/AA-SI_aalibrary/blob/main/other/inport_validations.ipynb).

The `permissions` table in BigQuery lists the user access-levels for the Metadata UI. which has not been deployed yet at the time of this writing, and is covered more in the next section.

Most metadata database functions for AALibrary are recorded within [`metadata.py`](https://github.com/nmfs-ost/AA-SI_aalibrary/blob/main/src/aalibrary/metadata.py).

## 6. Metadata UI

The Metadata UI is a browser-based application to help end-users in the AASI manage metadata. This includes interacting with the metadata database, and creating/validating/submitting metadata to Tugboat for archival.

The UI has been in development, and not currently deployed. You can visit the [project's repo](https://github.com/nmfs-ost/AA_SI_UI) to learn more.

## 7. Analysis Tools

There are various analysis tools created by AASI developers to satiate end-user needs. All of the analysis tools included in the AALibrary are located within, and accessible through the `console tools`.

### 7a. Console Tools

You can find more documentation on `console tools` through the [Console Tools](./console_tools.md) page, which lists all of the console tools, with their `help()` command printed out.

For achieving data provenance through console tools, please see the [Console Products & Provenance](./console_provenance.md) page.

For information on writing your own console tools, please visit the [Writing Console Tools](./console_tool_authoring.md) page.

### 7b. Recipe Manager

The Recipe Manager is a strong exploratory data analysis tool and is separate from AALibrary. A good summary of what the Recipe Manager is capable of is written below:

> aa-recipe-manager is a Python package that enables scientists to define, share, generate, and execute standardized scientific workflow recipes using declarative YAML files. It acts as a structured layer between scientists and their code by mapping parameters, referencing existing libraries, and generating runnable artifacts like Jupyter notebooks or Python scripts. Key features include a step registry for operations, round-trip parameter capturing, direct DAG execution with progress reporting, built-in check-pointing and result caching to avoid redundant computations, and distributed execution support via Dask, Prefect, or batch runners.

You can visit the [Recipe Manager repo](https://github.com/BLayman-NOAA/AA-SI_recipe_manager) to learn more.

### 7c. Workbench

The `Workbench` is a future-planned addition to the AALibrary. It consists of a UI that can run locally, and assist end-users with managing their `console tools` pipelines, and Recipe Manager recipes. This tool was meant to serve as another exploratory data analysis tool for acousticians.

You can find more information in the [Workbench repository](https://github.com/nmfs-ost/AA-SI_Workbench).

## Resources To Understand AASI Data Better

### Raw Files

More information on `.raw` files can be found on the [Echoview website](https://support.echoview.com/WebHelp/Reference/File_Formats/About_file_formats.htm) [see [Kongsberg Data Files](https://support.echoview.com/WebHelp/Reference/File_Formats/Kongsberg_data_files.htm), or [Simrad Data Files](https://support.echoview.com/WebHelp/Reference/File_Formats/Simrad_data_files.htm)].

There are numerous other resources available to understand `.raw` files better, just a quick search away.

### NetCDF Files

The [netCDF4 format](https://github.com/ices-publications/SONAR-netCDF4) is a file formatting convention for sonar data. NetCDF files usually end with a `.nc` extension. Raw files [get converted into netCDF](../usage/conversions.md) files, before they are used for analysis.

### Datagrams

Datagrams are 'packets' of metadata information that exist alongside raw data within a `.raw` file. AALibrary has [datagram parsing functionality built-in](https://github.com/nmfs-ost/AA-SI_aalibrary/blob/main/src/aalibrary/utils/get_datagram_data.py), allowing you to use datagrams in pipelines and any other code you wish.

Additionally, AALibrary provides users with the ability to stream the datagram data of a raw from either NCEI or GCP in-memory for faster read operations.
