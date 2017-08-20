.PHONY: docs

# test commands and arguments
tcommand = PYTHONPATH=. py.test -x
tmessy = -svv
targs = --cov-report term-missing --cov schemainspect

pip:
	pip install --upgrade pip
	pip install --upgrade -r requirements.txt

tox:
	tox tests

test:
	$(tcommand) $(targs) tests

stest:
	$(tcommand) $(tmessy) $(targs) tests

clean:
	git clean -fXd
	find . -name \*.pyc -delete

lint:
	flake8 schemainspect
	flake8 tests

tidy: clean lint

all: clean lint tox

upload:
	python setup.py sdist bdist_wheel --universal
	twine upload dist/*

publish: all upload
