Installation
------------

From source
^^^^^^^^^^^

Create virtualenv::

    $ mkvirtualenv batch_scoring

Install package in virtualenv::

    $ pip install -e .

Now ``batch_scoring`` script should be available in your PATH.

You can also create virtualenvs with different python versions::

    $ mkvirtualenv batch_scoring_3 -p /usr/bin/python3.5

Using Docker containers
~~~~~~~~~~~~~~~~~~~~~~~

Build containers::

    $ docker-compose build

Run tests in 2.7 and 3.5::

    $ docker-compose run python27 make test
    $ docker-compose run python35 make test

Run batch-scoring from container::

    $ docker-compose run python27 batch-scoring {args..}
    $ docker-compose run python35 batch-scoring {args..}

Deployment
----------

Cut a release candidate

  - update ``__version__`` in ``datarobot_batch_scoring/__init__.py``
  - perform acceptance tests
  - tag release
  - push a tag to GitHub

Travis bot runs automated tests and publish new version on PyPI when
tests are passed.


Offline Bundle
--------------

``make offlinebundle`` will create a zip file containing:

  - helper_packages/ - packages like pip and virtualenv which can be used offline
  - required_packages/ - the batch_scoring script and its dependencies
  - OFFLINE_INSTALL_README.txt - install instructions 
  - get-pip.py - a script that allows us to bootstrap pip in user mode

This offline install method is suitable in situations where Python 2.7 or Python 3 
are available. ``sudo`` is not required.


PyInstaller single-file executable - experimental
-------------------------------------------------

`See an overview of PyInstaller here <http://pyinstaller.readthedocs.io/en/stable/operating-mode.html>`_

This is still experimental, but it seems to work on linux and OSX. PyInstaller bundles
all the code and dependencies, including the Python interpreter, into a single
directory or executable file. Right now we are creating two single-file
executables; batch_scoring_sse and batch_scoring.

The requirements for building are virtualenv and python3.4+. Python 2.7 
should work, but we don't want to ship that at this point. 
To build, run ``make pyinstaller``. The executables will be placed in 
``./dist/datarobot_batch_scoring_x.y.z_executables.zip``.

This is considered experimental because it's untested, and may not work on every platform
we need to support. For example, we need to be careful that linux apps are
forward compatible_, and we would need separate builds_ for OSX and Windows.

.. _compatible: http://pyinstaller.readthedocs.io/en/stable/usage.html#making-linux-apps-forward-compatible
.. _builds: http://pyinstaller.readthedocs.io/en/stable/usage.html#supporting-multiple-operating-systems
