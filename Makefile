.PHONY: all test release

all:
	@echo 'test     run the unit tests with the current default python'
	@echo 'release  publish the current version to pypi'

test:
	@pytest

release:
	@python ./setup.py sdist upload
