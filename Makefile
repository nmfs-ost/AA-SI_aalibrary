# For building and distributing this library as a package.
dist:
    pip install --upgrade build
    pip install twine
    pip install keyring
    pip install keyrings.google-artifactregistry-auth
    # bottom command should specify ChainerBackend(priority:10) & GooglePythonAuth(priority: 9)
    keyring --list-backends
    rm -r dist/*
    python -m build
    twine upload --repository-url https://us-central1-python.pkg.dev/ggn-nmfs-aa-dev-1/aalibrary/ dist/*

install:
    pip install keyring
    pip install keyrings.google-artifactregistry-auth
    keyring --list-backends
    python -m pip uninstall aalibrary -y
    python -m pip install --index-url https://us-central1-python.pkg.dev/ggn-nmfs-aa-dev-1/aalibrary/simple/ aalibrary
    conda list | pip list

update-changelog:
    git-cliff --config cliff.toml --repository . -o --tag 0.1.0 --bump

local-build-and-install:
    rm -r dist/*
    python -m build
    python -m pip uninstall aalibrary -y
    python -m pip install dist/*.whl
