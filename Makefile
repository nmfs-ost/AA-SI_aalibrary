# For building and distributing this library as a package.
distribute:
	pip install --upgrade build
	pip install twine
	pip install keyring
	pip install keyrings.google-artifactregistry-auth
	# bottom command should specify ChainerBackend(priority:10) & GooglePythonAuth(priority: 9)
	keyring --list-backends
	rm -rf dist/*
	python -m build
	twine upload --repository-url https://us-central1-python.pkg.dev/ggn-nmfs-aa-dev-1/aalibrary/ dist/*

uninstall:
	pip uninstall aalibrary -y

install:
	pip install keyring
	pip install keyrings.google-artifactregistry-auth
	keyring --list-backends
	python -m pip uninstall aalibrary -y
	python -m pip install --index-url https://us-central1-python.pkg.dev/ggn-nmfs-aa-dev-1/aalibrary/simple/ aalibrary --extra-index-url https://pypi.python.org/simple
	conda list | pip list

update-changelog:
	git-cliff --config cliff.toml --repository . -o --tag 0.1.0 --bump

local-build-and-install:
	rm -rf dist/*
	python -m build
	python -m pip uninstall aalibrary -y
	python -m pip install dist/*.whl

pytest: uninstall-editable install-editable
	# to make sure we are testing the current code, not the installed code.
	python -m pytest .

mkdocs: install-editable
	mkdocs serve --livereload

mkdocs-publish: install-editable
	echo "Make sure you are in the main branch when executing. Changes get automatically published to the Github Pages Site."
	mkdocs gh-deploy --force 

install-editable:
	python -m pip install -e .

uninstall-editable:
	python -m pip uninstall aalibrary -y

create-tugboat-submission-cloudrun-function:
	gcloud config set project ggn-nmfs-aa-dev-1
	gcloud run deploy tugboat-submission-function \
	--source=. \
	--env-vars-file=env.yaml \
	--function=handle_request \
	--base-image=python313 \
	--region=us-east4 \
	--service-account="tugboat-run-sa1@ggn-nmfs-aa-dev-1.iam.gserviceaccount.com" \
    --build-service-account="projects/ggn-nmfs-aa-dev-1/serviceAccounts/cloudbuild-sa-1@ggn-nmfs-aa-dev-1.iam.gserviceaccount.com" \
	--ingress=internal-and-cloud-load-balancing \
	--vpc-connector="serverless-vpc-connector1" \
	--vpc-egress="all-traffic" \
	--no-allow-unauthenticated
