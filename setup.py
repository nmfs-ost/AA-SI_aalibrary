from setuptools import setup, find_packages

# Get version number
with open("./src/aalibrary/about.py") as f:
    info = {}
    for line in f:
        if line.startswith("__version__"):
            exec(line, info)
            break

setup_info = dict(
    name='aalibrary',
    version=info['__version__'],
    author='Hannan Khan',
    author_email='hannan.khan@noaa.gov',
    url='https://github.com/nmfs-ost/AA-SI_aalibrary',
    download_url='https://github.com/nmfs-ost/AA-SI_aalibrary',
    project_urls={
        #'Documentation': 'https://pyglet.readthedocs.io/en/latest',
        'Source': 'https://github.com/nmfs-ost/AA-SI_aalibrary',
        #'Tracker': 'https://github.com/pyglet/pyglet/issues',
    },
    description='A python library used for fetching acoustics data.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    license='Apache-2.0',
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
    packages=['aalibrary'] + ['aalibrary.' + pkg for pkg in find_packages('aalibrary')],

    # Add _ prefix to the names of temporary build dirs
    # options={'build': {'build_base': '_build'}, },
    # zip_safe=True,
)

setup(**setup_info)
