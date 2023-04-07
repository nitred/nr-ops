PYPROJECT_VERSION=$(shell poetry version -s)
NR_OPS_VERSION=$(shell python -c "import pkg_resources; print(pkg_resources.get_distribution('nr-ops').version)")
VAR_TAG_VERSION=${TAG_VERSION}
VAR_BASE_IMAGE_NAME=${BASE_IMAGE_NAME}


.PHONY : version_check_pyproject_vs_ci_commit_tag
version_check_pyproject_vs_ci_commit_tag:
	python -c "import nr_ops, sys; print('Check successful between PYPROJECT_VERSION == VAR_CI_COMMIT_TAG') if ('$(PYPROJECT_VERSION)' == '$(VAR_CI_COMMIT_TAG)') else sys.exit(1)"


.PHONY : version_check_pyproject_vs_init
version_check_pyproject_vs_init:
	python -c "import nr_ops, sys; print('Check successful between PYPROJECT_VERSION == NR_OPS_VERSION') if ('$(PYPROJECT_VERSION)' == '$(NR_OPS_VERSION)') else sys.exit(1)"


.PHONY : version_check_pre_tag
version_check_pre_tag: version_check_pyproject_vs_init
	python -c "import nr_ops, sys; print('Check successful between VAR_TAG_VERSION == NR_OPS_VERSION') if ('$(VAR_TAG_VERSION)' == '$(NR_OPS_VERSION)') else sys.exit(1)"


.PHONY : build_docker
build_docker:
	if [ -z "${VAR_BASE_IMAGE_NAME}" ]; then echo "VAR_BASE_IMAGE_NAME is empty"; exit 1; fi
	if [ -z "${VAR_TAG_VERSION}" ]; then echo "VAR_TAG_VERSION is empty"; exit 1; fi
	docker build -f docker/Dockerfile-prod -t ${VAR_BASE_IMAGE_NAME}:${VAR_TAG_VERSION} .


.PHONY : build_docker_lambda
build_docker_lambda:
	if [ -z "${VAR_BASE_IMAGE_NAME}" ]; then echo "VAR_BASE_IMAGE_NAME is empty"; exit 1; fi
	if [ -z "${VAR_TAG_VERSION}" ]; then echo "VAR_TAG_VERSION is empty"; exit 1; fi
	docker build -f docker/Dockerfile-prod-lambda -t ${VAR_BASE_IMAGE_NAME}:${VAR_TAG_VERSION}-lambda .
