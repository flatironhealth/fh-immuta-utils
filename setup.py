# coding: utf-8

from setuptools import setup, find_packages  # noqa: H301

NAME = "fh-immuta-utils"
VERSION = "0.0.1"
# To install the library, run the following
#
# python setup.py install
#
# prerequisite: setuptools
# http://pypi.python.org/pypi/setuptools

REQUIRES = ["urllib3 >= 1.15", "six >= 1.10", "certifi", "hvac", "python-dateutil"]

setup(
    name=NAME,
    version=VERSION,
    description="Flatiron Immuta API",
    author_email="data-tooling@flatiron.com",
    url="",
    install_requires=REQUIRES,
    packages=find_packages(),
    include_package_data=True,
    long_description="""\
    Wrapper around Immuta's API to manage data sources and permissions in Immuta.
    """,
)
