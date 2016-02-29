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


Deployment
----------

1. Cut a release candidate

  - update setup.py & datarobot_scoring/batch_scoring.py
  - acceptance testing
  - tag release (& push tag)

2. Create a source and wheel distributions::

    $ python setup.py sdist bdist_wheel

3. Distribute artifacts using `twine <https://pypi.python.org/pypi/twine>`_::

    $ twine upload dist/datarobot_batch_scoring-X.X.X.tar.gz

  Contact `@bratchenko <https://github.com/bratchenko>`_ for access to pypi.

Packaging for windows
---------------------
Use Python 3.4 only -- py2exe doesn't work with Python 3.5:

1. install py2exe (pip install py2exe)
2. install requirements (pip install -r requirements34.txt)
3. build dist (python setup.py py2exe)
4. Distribute content of *dist* folder
5. On target machine download "Microsoft Visual C++ 2010 Redistributable Package (x64)" (https://www.microsoft.com/en-us/download/details.aspx?id=14632) and install
6. Enjoy!
