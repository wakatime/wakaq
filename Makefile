.PHONY: all test clean build upload

all:
	@echo 'test     run the unit tests with the current default python'
	@echo 'clean    remove builds at dist/*'
	@echo 'build    run setup.py dist'
	@echo 'upload   upload all builds in dist folder to pypi'
	@echo 'release  publish the current version to pypi'

test:
	@pytest

release: clean build rename upload

clean:
	@rm -f dist/*

build:
	@python ./setup.py sdist

rename:
	for f in dist/WakaQ-*.tar.gz; do mv "$$f" "$$(echo "$$f" | sed s/WakaQ/wakaq/)"; done

upload:
	@twine upload ./dist/*
