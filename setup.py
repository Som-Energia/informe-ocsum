#!/usr/bin/env python

from setuptools import setup, find_packages
setup(
    name = "SwitchingReports",
    version = "0.2",
    packages = find_packages(),
    scripts = [
        'atr_debugcase',
        'atr_switchingreport',
        ],
    install_requires=open('requirements.txt').read().split('\n'),
    author = "SomEnergia SCCL",
    author_email = "it@somenergia.coop",
    description = "Spanish Electricity Retailer Switching Report Generator",
    license = "GPLv3",
    keywords = "electricity market retailer switching",
    url = "https://github.com/Som-Energia/informe-ocsum",
    # long_description, download_url, classifiers
    test_suite='switchingreport',
)

