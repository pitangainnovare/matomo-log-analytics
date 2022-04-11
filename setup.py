#!/usr/bin/env python
from setuptools import setup, find_packages

install_requires = [
    'sqlalchemy',
    'mysqlclient',
    'python-magic',
]


setup(
    name="scielo-matomo-manager",
    version='0.2.2',
    description="The SciELO Matomo Manager",
    author="SciELO",
    author_email="scielo-dev@googlegroups.com",
    license="BSD",
    url="https://github.com/scieloorg/matomo-log-analytics",
    keywords='log manager',
    maintainer_email='rafael.pezzuto@gmail.com',
    packages=find_packages(),
    install_requires=install_requires,
    entry_points="""
    [console_scripts]
    initialize_database=proc.initialize_database:main
    update_available_logs=proc.update_available_logs:main
    load_logs=proc.load_logs:main
    clean_data=proc.clean_data:main
    """
)
