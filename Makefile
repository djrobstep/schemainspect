.PHONY: docs

# test commands and arguments
tcommand = PYTHONPATH=. py.test -x
tmessy = -svv
targs = --cov-report term-missing --cov schemainspect

pip:
	pip install --upgrade pip
	pip install --upgrade --upgrade-strategy=eager -r requirements.txt

tox:
	tox tests

test:
	$(tcommand) $(targs) tests

stest:
	$(tcommand) $(tmessy) $(targs) tests

clean:
	git clean -fXd
	find . -name \*.pyc -delete

fmt:
	black .

lint:
	flake8 schemainspect
	flake8 tests

tidy: clean fmt lint

all: tidy tox

upload:
	python setup.py sdist bdist_wheel --universal
	twine upload dist/*

publish: pip tidy tox clean upload
