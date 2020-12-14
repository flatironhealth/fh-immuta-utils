# coding: utf-8

from setuptools import setup, find_packages  # noqa: H301

NAME = "fh-immuta-utils"


with open("README.md", "r") as fh:
    long_description = fh.read()


setup(
    name=NAME,
    version="0.3.0",
    entry_points={
        "console_scripts": ["fh-immuta-utils = fh_immuta_utils.scripts.cli:main_cli"]
    },
    description="Flatiron Immuta API",
    author_email="data-tooling@flatiron.com",
    url="https://github.com/flatironhealth/fh-immuta-utils",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "click",
        "hvac",
        "pydantic",
        "PyYAML",
        "requests",
        "six",
        "toolz",
        "tqdm",
        "urllib3",
    ],
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        "Programming Language :: Python :: 3 :: Only",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Development Status :: 5 - Production/Stable",
    ],
    python_requires=">=3.6",
)
