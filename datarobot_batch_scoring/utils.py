import six

input = six.moves.input


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
