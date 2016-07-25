.PHONY: docs

# test commands and arguments
tcommand = PYTHONPATH=. py.test -x
tmessy = -svv
targs = --cov-report term-missing --cov schemainspect

pip:
	pip install -r requirements-dev.txt

pipupgrade:
	pip install --upgrade pip
	pip install --upgrade -r requirements-dev.txt

pipreqs:
	pip install -r requirements.txt

pipeditable:
	pip install -e .

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

docs:
	cd docs && make clean && make html

opendocs:
	BROWSER=firefox python -c 'import os;import webbrowser;webbrowser.open_new_tab("file://" + os.getcwd() + "/docs/_build/html/index.html")'

tidy: clean lint

all: clean lint tox

testpublish:
	python setup.py register -r https://testpypi.python.org/pypi
	python setup.py sdist bdist_wheel --universal upload -r https://testpypi.python.org/pypi

publish:
	python setup.py register
	python setup.py sdist bdist_wheel --universal upload
