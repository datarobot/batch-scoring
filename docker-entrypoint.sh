#!/bin/sh

# do editable install in case of clean sources mounted inside container
[ -f setup.py ] && [ -d datarobot_batch_scoring ] && [ ! -d datarobot_batch_scoring.egg-info ] && pip install .

exec "$@"
