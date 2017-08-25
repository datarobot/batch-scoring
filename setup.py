#!/usr/bin/env python
import codecs
import os.path
import re
from setuptools import setup, find_packages

extra = {}


fname = os.path.join(os.path.abspath(os.path.dirname(
    __file__)), 'requirements.txt')

install_requires = open(fname, 'r').readlines()

extra['entry_points'] = {
    'console_scripts': [
        'batch_scoring = datarobot_batch_scoring.main:main',
        'batch_scoring_sse = datarobot_batch_scoring.main:main_standalone'
    ]}
extra['install_requires'] = install_requires


fname = os.path.join(os.path.abspath(os.path.dirname(
    __file__)), 'datarobot_batch_scoring', '__init__.py')


with codecs.open(fname, 'r', 'latin1') as fp:
    try:
        version = re.findall(r"^__version__ = '([^']+)'\r?$",
                             fp.read(), re.M)[0]
    except IndexError:
        raise RuntimeError('Unable to determine version.')


setup(
    name='datarobot_batch_scoring',
    version=version,
    description=("A script to score CSV files via DataRobot's prediction API"),
    author='DataRobot',
    author_email='support@datarobot.com',
    maintainer='DataRobot',
    maintainer_email='support@datarobot.com',
    license='BSD',
    url='http://www.datarobot.com/',
    packages=find_packages(),
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
    **extra
)
