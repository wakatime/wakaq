.PHONY: all test clean build upload

all:
	@echo 'test     run the unit tests with the current default python'
	@echo 'clean    remove builds at dist/*'
	@echo 'build    run setup.py dist'
	@echo 'upload   upload all builds in dist folder to pypi'
	@echo 'release  publish the current version to pypi'

test:
	@pytest

release: clean build upload

clean:
	@rm -f dist/*

build:
	@python ./setup.py sdist

upload:
	@twine upload ./dist/*
