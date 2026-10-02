# One-Page Overview for AALibrary

This one-pager document covers all that is necessary to get started with AALibrary and the console tools.

## 1. Permissions

Each portion/project of the AASI requires separate permissions. You can find instructions for obtaining permissions to the various aspects of AALibrary [on the permissions page](../getting-started/permissions.md).

## 2. Installation

The installation of AALibrary varies by the operating system. The console tools come bundled with AALibrary, and are accessible as command line commands. You can find more instructions on how to [install AALibrary here](../getting-started/installation.md).

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

There is an external product also being actively developed by echostack named [`echodataflow`](https://github.com/echostack-org/echodataflow). However, this product is currently better suited for immediate data processing and analysis (i.e. to achieve near-real-time analysis).

#### Data Archival To NCEI

The overall goal of AASI is to get data up into the cloud. This is done by uploading data to GCP, generating the correct calibration metadata and such, and then archiving the data in NCEI.

The data we have archived so far is publicly available, and can be seen on the [NCEI S3 Bucket](https://noaa-wcsd-pds.s3.amazonaws.com/index.html#data/raw/).

The tool used to archive GCP data to NCEI is called Tugboat. Tugboat requires a JSON file filled with survey metadata. Tugboat then pulls the data from GCP Storage Buckets.More information can be found on the [Tugboat Integration page](../usage/tugboat.md).

#### Overall Architecture

More details on the data lifecycle architecture implementation can be found through [this diagram](https://drive.google.com/file/d/1kleBzjKXvX-mJejetF4qljxkSFSp18Uy/view?usp=sharing).

## 4. Data Storage Standards In GCP

## 5. Metadata DB

## 6. Metadata UI

## 7. Analysis Tools

### 7a. Console Tools

### 7b. Recipe Manager

### 7c. Workbench

## Extra Resources To Understand AASI Data Better
raw files
netcdf files
datagrams
etc.