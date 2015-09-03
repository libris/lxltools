#!/usr/bin/env python
# -*- coding: UTF-8 -*-
from os import path as P
from setuptools import setup
import lddb


with open(P.join(P.dirname(__file__), 'requirements.txt')) as fp:
    requirements = [l.rstrip() for l in fp.readlines()]

setup(
    name = "LDDB",
    version = lddb.__version__,
    description = """LIBRISXL Linked Data Database""",
    long_description = """
    %s""" % "".join(open("README.md")),
    classifiers = [
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Python Modules"
        ],
    keywords = "linkeddata database json rdf",
    platforms = ["any"],
    packages = ["lddb"],
    include_package_data = True,
    install_requires = requirements,
    test_suite = 'nose.collector'
)
