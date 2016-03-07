from functools import partial
from os.path import expanduser, isfile, join as path_join
from os import getcwd

import six
import trafaret as t
from six.moves.configparser import ConfigParser

OptKey = partial(t.Key, optional=True)

input = six.moves.input

CONFIG_FILENAME = '.batch_scoring.ini'


def verify_objectid(id_):
    """Verify if id_ is a proper ObjectId. """
    if not len(id_) == 24:
        raise ValueError('id {} not a valid project/model id'.format(id_))


config_validator = t.Dict({
    OptKey('host'): t.String,
    OptKey('project_id'): t.contrib.object_id.MongoId >> str,
    OptKey('model_id'): t.contrib.object_id.MongoId >> str,
    OptKey('n_retry'): t.Int,
    OptKey('keep_cols'): t.String,
    OptKey('n_concurrent'): t.Int,
    OptKey('dataset'): t.String,
    OptKey('n_samples'): t.Int,
    OptKey('delimiter'): t.String,
    OptKey('out'): t.String,
    OptKey('user'): t.String,
    OptKey('password'): t.String,
    OptKey('datarobot_key'): t.String,
    OptKey('timeout'): t.Int,
    OptKey('api_token'): t.String,
    OptKey('create_api_token'): t.String,
    OptKey('pred_name'): t.String,
    OptKey('api_version'): t.Enum('v1', 'v2')
}).allow_extra('*')


class UI(object):

    def __init__(self, prompt):
        self._prompt = prompt

    def prompt_yesno(self, msg):
        if self._prompt is not None:
            return self._prompt
        cmd = input('{} (yes/no)> '.format(msg)).strip().lower()
        while cmd not in ('yes', 'no'):
            cmd = input('Please type (yes/no)> ').strip().lower()
        return cmd == 'yes'

    def prompt_user(self):
        return input('user name> ').strip()


def get_config_file():
    """
    Lookup for config file at user home directory or working directory.
    Returns
    -------
    str or None
        Path to config file or None if it not exists.
    """
    home_path = path_join(expanduser('~'), CONFIG_FILENAME)
    cwd_path = path_join(getcwd(), CONFIG_FILENAME)
    if isfile(home_path):
        return home_path
    elif isfile(cwd_path):
        return cwd_path
    return None


def parse_config_file(file_path):
    config = ConfigParser()
    config.read(file_path)
    if 'batch_scoring' not in config.sections():
        # We are return empty dict, because there is nothing in this file
        # that related to arguments to batch scoring.
        return {}
    parsed_dict = dict(config.items('batch_scoring'))
    return config_validator(parsed_dict)
