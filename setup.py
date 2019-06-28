#!/bin/env python
import os.path
from setuptools import setup, find_packages

setup(
    name='schematic',
    version='0.1',
    include_package_data=True,
    packages=find_packages(),
    entry_points={
            'console_scripts': [
                'schematic=schematic:cli'
            ]
    },
    install_requires=[
        "click",
        "boto3",
        "pyyaml",
        "psycopg2-binary",
    ]
)
