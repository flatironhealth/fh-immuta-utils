# coding: utf-8

from setuptools import setup, find_packages  # noqa: H301

NAME = "fh-immuta-utils"
VERSION = "0.0.1"


with open("README.md", "r") as fh:
    long_description = fh.read()


setup(
    name=NAME,
    version=VERSION,
    description="Flatiron Immuta API",
    author_email="data-tooling@flatiron.com",
    url="https://github.com/flatironhealth/fh-immuta-utils",
    packages=find_packages(),
    include_package_data=True,
    long_description=long_description,
    classifiers = [
        "Programming Language :: Python :: 3 :: Only",
        "License  :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Development Status :: 5 - Production/Stable",
    ],
    python_requires='>=3.6',
)
