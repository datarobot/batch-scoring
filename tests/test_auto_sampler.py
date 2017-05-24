import mock

from datarobot_batch_scoring.reader import (investigate_encoding_and_dialect,
                                            auto_sampler)


class TestAutoSampler(object):

    def test_super_wide_dataset_is_auto_sample_10(self,
                                                  csv_file_with_wide_dataset):
        """PRED 1240"""
        enc = investigate_encoding_and_dialect(
            dataset=csv_file_with_wide_dataset,
            sep=',',
            ui=mock.Mock(),
            fast=False,
            encoding='utf-8',
            skip_dialect=True)

        s = auto_sampler(dataset=csv_file_with_wide_dataset,
                         encoding=enc,
                         ui=mock.Mock())
        assert 10 == s
