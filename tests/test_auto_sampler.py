import os
import tempfile

import mock

from datarobot_batch_scoring.reader import (investigate_encoding_and_dialect,
                                            auto_sampler)

path = '/home/dallin/tmp/PRED-1240/head.csv'


class TestAutoSampler(object):

    def test_super_wide_dataset_is_auto_sample_1(self,
                                                 csv_file_with_wide_dataset):
        """PRED 1240"""
        with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as f:
            f.write(csv_file_with_wide_dataset.getvalue())
        try:
            enc = investigate_encoding_and_dialect(dataset=f.name,
                                                   sep=',',
                                                   ui=mock.Mock(),
                                                   fast=False,
                                                   encoding='utf-8',
                                                   skip_dialect=True)

            s = auto_sampler(dataset=f.name, encoding=enc, ui=mock.Mock())
            assert 10 == s
        finally:
            os.remove(f.name)
