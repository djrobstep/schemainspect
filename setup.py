#!/usr/bin/env python
import io

from setuptools import setup, find_packages

setup(
    name="schemainspect",
    version="0.1.1535502343",
    url="https://github.com/djrobstep/schemainspect",
    description="Schema inspection for PostgreSQL",
    long_description=io.open("README.rst").read(),
    author="Robert Lechte",
    author_email="robertlechte@gmail.com",
    install_requires=["six", "sqlalchemy"],
    zip_safe=False,
    packages=find_packages(),
    package_data={"schemainspect": ["pg/*.sql"]},
    classifiers=["Development Status :: 4 - Beta"],
    extras_require={"pg": ["psycopg2-binary"]},
)
