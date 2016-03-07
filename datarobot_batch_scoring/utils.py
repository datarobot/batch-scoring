import logging
import os
import six
import sys
import tempfile

input = six.moves.input


logger = logging.getLogger('main')
root_logger = logging.getLogger()


class UI(object):

    def __init__(self, prompt, loglevel):
        self._prompt = prompt
        self._configure_logging(loglevel)

    def _configure_logging(self, level):
        """Configures logging for user and debug logging. """

        with tempfile.NamedTemporaryFile(prefix='datarobot_batch_scoring_',
                                         suffix='.log', delete=False) as fd:
            pass
        self.root_logger_filename = fd.name

        # user logger
        fs = '[%(levelname)s] %(message)s'
        hdlr = logging.StreamHandler()
        dfs = None
        fmt = logging.Formatter(fs, dfs)
        hdlr.setFormatter(fmt)
        logger.setLevel(level)
        logger.addHandler(hdlr)

        # root logger
        fs = '%(asctime)-15s [%(levelname)s] %(message)s'
        hdlr = logging.FileHandler(self.root_logger_filename, 'w+')
        dfs = None
        fmt = logging.Formatter(fs, dfs)
        hdlr.setFormatter(fmt)
        root_logger.setLevel(logging.DEBUG)
        root_logger.addHandler(hdlr)

    def prompt_yesno(self, msg):
        if self._prompt is not None:
            return self._prompt
        cmd = input('{} (yes/no)> '.format(msg)).strip().lower()
        while cmd not in ('yes', 'no'):
            cmd = input('Please type (yes/no)> ').strip().lower()
        return cmd == 'yes'

    def prompt_user(self):
        return input('user name> ').strip()

    def debug(self, msg):
        logger.debug(msg)

    def info(self, msg):
        logger.info(msg)

    def warning(self, msg):
        logger.warning(msg)

    def error(self, msg):
        logger.error(msg)
        if sys.exc_info()[0]:
            exc_info = True
        else:
            exc_info = False
        root_logger.error(msg, exc_info=exc_info)

    def fatal(self, msg):
        msg = ('{}\nIf you need assistance please send the log \n'
               'file {} to support@datarobot.com .').format(
                   msg, self.root_logger_filename)
        logger.error(msg)
        exc_info = sys.exc_info()
        root_logger.error(msg, exc_info=exc_info)
        sys.exit(1)

    def close(self):
        os.unlink(self.root_logger_filename)
