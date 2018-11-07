.PHONY: docs

# test commands and arguments
tcommand = py.test -x
tmessy = -svv
targs = --cov-report term-missing --cov schemainspect

check: clean fmt test lint

test:
	$(tcommand) $(targs) tests

stest:
	$(tcommand) $(tmessy) $(targs) tests

clean:
	git clean -fXd
	find . -name \*.pyc -delete

fmt:
	isort -rc .
	black .

lint:
	flake8 .
