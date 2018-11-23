#!/usr/bin/env python
import sys
import codecs
import os.path
import re
from setuptools import setup, find_packages

extra = {}


fname = os.path.join(os.path.abspath(os.path.dirname(
    __file__)), 'requirements.txt')

install_requires = open(fname, 'r').readlines()

# Since futures 3.2 [1], the package enforces to be installed only in Python 2
# environments because it's basically a backport of Python 3's built-in
# package. So in order to support both Python 2 and Python 3 environments, we
# have to skip installation of futures package in case of Python 3.
#
# It might look natural to use environment markers [2] to achieve this goal but
# they are new and were introduced in setuptools in mid of 2017. FWIW,
# setuptools on both Ubuntu Trusty and Ubuntu Xenial do not support them and
# batch scoring script may be installed in pretty outdated envs. So let's do it
# old-fashioned way by adding condition here.
#
# [1] https://github.com/agronholm/pythonfutures/commit/d0393ad626d25622927bb0ed47d35ddb2f6cd321 # noqa: E501
# [2] https://www.python.org/dev/peps/pep-0508/#environment-markers
if sys.version_info[0] > 2:
    install_requires = [req
                        for req in install_requires
                        if not req.startswith('futures')]

extra['entry_points'] = {
    'console_scripts': [
        'batch_scoring = datarobot_batch_scoring.main:main',
        'batch_scoring_sse = datarobot_batch_scoring.main:main_standalone',
        'batch_scoring_deployment_aware = datarobot_batch_scoring.main:main_deployment_aware'
    ]}
extra['install_requires'] = install_requires


this_directory = os.path.abspath(os.path.dirname(__file__))


init_fname = os.path.join(this_directory, 'datarobot_batch_scoring', '__init__.py')
with codecs.open(init_fname, 'r', 'latin1') as fp:
    try:
        version = re.findall(r"^__version__ = '([^']+)'\r?$",
                             fp.read(), re.M)[0]
    except IndexError:
        raise RuntimeError('Unable to determine version.')


readme_fname = os.path.join(this_directory, 'README.rst')
with codecs.open(readme_fname, 'r', 'utf-8') as f:
    long_description = f.read()


setup(
    name='datarobot_batch_scoring',
    version=version,
    description=("A script to score CSV files via DataRobot's prediction API"),
    long_description=long_description,
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
