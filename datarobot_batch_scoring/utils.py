import six

input = six.moves.input


def prompt_yesno(msg):
    cmd = input('{} (yes/no)> '.format(msg)).strip().lower()
    while cmd not in ('yes', 'no'):
        cmd = input('Please type (yes/no)> ').strip().lower()
    return cmd == 'yes'


def prompt_user():
    return input('user name> ').strip()
