import csv

import pytest
from mock import Mock

from datarobot_batch_scoring.reader import FastReader, SlowReader


class TestCSVReaderWithWideData(object):

    @pytest.fixture(autouse=True)
    def registered_dialect(self):
        # This was written weird
        csv.register_dialect('dataset_dialect', csv.excel)

    @pytest.yield_fixture
    def really_big_fields_enabled(self):
        old_limit = csv.field_size_limit()
        csv.field_size_limit(2 ** 28)
        yield
        csv.field_size_limit(old_limit)

    def test_slow_reader_with_really_wide_field_fails_default(
            self, csv_file_handle_with_wide_field):
        reader = SlowReader(csv_file_handle_with_wide_field,
                            'utf-8',
                            ui=Mock())
        with pytest.raises(csv.Error):
            list(reader)

    @pytest.mark.usefixtures('really_big_fields_enabled')
    def test_slow_reader_can_succeed_if_override_csv_width(
            self, csv_file_handle_with_wide_field):
        reader = SlowReader(csv_file_handle_with_wide_field,
                            'utf-8',
                            ui=Mock())
        data = list(reader)
        assert len(data) == 4

    def test_fast_reader_with_really_wide_field_wont_even_instantiate_default(
            self,
            csv_file_handle_with_wide_field):
        with pytest.raises(csv.Error):
            FastReader(csv_file_handle_with_wide_field, 'utf-8', ui=Mock())

    @pytest.mark.usefixtures('really_big_fields_enabled')
    def test_fast_reader_can_succeed_if_override_csv_width(
            self,
            csv_file_handle_with_wide_field):
        reader = FastReader(csv_file_handle_with_wide_field,
                            'utf-8',
                            ui=Mock())
        data = list(reader)
        assert len(data) == 4


class TestCSVReaderWithTerminators(object):

    def test_csv_file_with_cr_fast(self, csv_data_with_cr):
        reader = FastReader(csv_data_with_cr, 'utf-8', ui=Mock())
        data = list(reader)
        assert len(data) == 3

    def test_csv_file_with_cr_slow(self, csv_data_with_cr):
        reader = SlowReader(csv_data_with_cr, 'utf-8', ui=Mock())
        data = list(reader)
        assert len(data) == 3

