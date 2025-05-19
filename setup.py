from setuptools import setup, find_namespace_packages

# Get version number
with open("./src/aalibrary/about.py") as f:
    info = {}
    for line in f:
        if line.startswith("__version__"):
            exec(line, info)
            break

setup_info = dict(
    name="aalibrary",
    version=info["__version__"],
    author="Hannan Khan",
    author_email="hannan.khan@noaa.gov",
    url="https://github.com/nmfs-ost/AA-SI_aalibrary",
    download_url="https://github.com/nmfs-ost/AA-SI_aalibrary",
    project_urls={
        # 'Documentation': 'https://pyglet.readthedocs.io/en/latest',
        "Source": "https://github.com/nmfs-ost/AA-SI_aalibrary",
        # 'Tracker': 'https://github.com/pyglet/pyglet/issues',
    },
    description="A python library used for fetching acoustics data.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    license="Apache-2.0",
    # classifiers=[
    #     'Development Status :: 5 - Production/Stable',
    #     'Environment :: MacOS X',
    #     'Environment :: Win32 (MS Windows)',
    #     'Environment :: X11 Applications',
    #     'Intended Audience :: Developers',
    #     'License :: OSI Approved :: BSD License',
    #     'Operating System :: MacOS :: MacOS X',
    #     'Operating System :: Microsoft :: Windows',
    #     'Operating System :: POSIX :: Linux',
    #     'Programming Language :: Python :: 3',
    #     'Programming Language :: Python :: 3.8',
    #     'Programming Language :: Python :: 3.9',
    #     'Programming Language :: Python :: 3.10',
    #     'Programming Language :: Python :: 3.11',
    #     'Topic :: Games/Entertainment',
    #     'Topic :: Software Development :: Libraries :: Python Modules',
    # ],
    # Package info
    packages=find_namespace_packages(
        where="src/", include=["aalibrary.*"]
    ),
    package_dir={"": "src"},
    # Requirements
    install_requires=[
        "echopype>=0.9.0",
        "zarr==2.18.3"
        "boto3>=1.35.29",
        "Flask>=3.0.3",
        "gcsfs",
        "google-api-python-client",
        "google-cloud",
        "google-cloud-bigquery",
        "google-cloud-storage",
        "google-cloud-vision",
        "numpy>=1.26.4",
        "openpyxl>=3.1.5",
        "pandas>=2.2.2",
        "pandas_gbq>=0.22.0",
        "pytest>=8.3.3",
        "pytz>=2024.1",
        "tqdm>=4.66.2",
        "azure-storage-file-datalake>=12.18.0",
    ],
    # Add _ prefix to the names of temporary build dirs
    # options={'build': {'build_base': '_build'}, },
    # zip_safe=True,
    
)

setup(**setup_info)
