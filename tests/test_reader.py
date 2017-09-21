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


class TestCSVDataReaderWithTerminators(object):
    """ Class of tests to handle text with terminators """

    def test_csv_data_with_cr_fast(self, csv_data_with_cr):
        """ Fast and CR """
        reader = FastReader(csv_data_with_cr, 'utf-8', ui=Mock())
        data = list(reader)
        assert len(data) == 3

    def test_csv_data_with_cr_slow(self, csv_data_with_cr):
        """ Slow and CR """
        reader = SlowReader(csv_data_with_cr, 'utf-8', ui=Mock())
        data = list(reader)
        assert len(data) == 3

    def test_csv_data_file_with_crlf_fast(self, csv_data_with_crlf):
        """ Fast and CRLF """
        reader = FastReader(csv_data_with_crlf, 'utf-8', ui=Mock())
        data = list(reader)
        assert len(data) == 3

    def test_csv_data_with_crlf_slow(self, csv_data_with_crlf):
        """ Slow and CRLF """
        reader = SlowReader(csv_data_with_crlf, 'utf-8', ui=Mock())
        data = list(reader)
        assert len(data) == 3

    def test_csv_data_file_with_lf_fast(self, csv_data_with_lf):
        """ Fast and LF """
        reader = FastReader(csv_data_with_lf, 'utf-8', ui=Mock())
        data = list(reader)
        assert len(data) == 3

    def test_csv_data_with_lf_slow(self, csv_data_with_lf):
        """ Slow and LF """
        reader = SlowReader(csv_data_with_lf, 'utf-8', ui=Mock())
        data = list(reader)
        assert len(data) == 3


class TestCSVFileReaderWithTerminators(object):
    """
    Class of tests to handle files with terninators
    In large measure, this is a test that open("rU") does a
    suitable translation to text flowing through.
    """
    def test_csv_file_with_cr_fast(self, csv_file_with_cr):
        """ Fast and CR """
        with open(csv_file_with_cr, "rU") as handle:
            reader = FastReader(handle, 'utf-8', ui=Mock())
            data = list(reader)
            assert len(data) == 3

    def test_csv_file_with_cr_slow(self, csv_file_with_cr):
        """ Slow and CR """
        with open(csv_file_with_cr, "rU") as handle:
            reader = SlowReader(handle, 'utf-8', ui=Mock())
            data = list(reader)
            assert len(data) == 3

    def test_csv_file_with_crlf_fast(self, csv_file_with_crlf):
        """ Fast and CRLF """
        with open(csv_file_with_crlf, "rU") as handle:
            reader = FastReader(handle, 'utf-8', ui=Mock())
            data = list(reader)
            assert len(data) == 3

    def test_csv_file_with_crlf_slow(self, csv_file_with_crlf):
        """ Slow and CRLF """
        with open(csv_file_with_crlf, "rU") as handle:
            reader = SlowReader(handle, 'utf-8', ui=Mock())
            data = list(reader)
            assert len(data) == 3

    def test_csv_file_with_lf_fast(self, csv_file_with_lf):
        """ Fast and LF """
        with open(csv_file_with_lf, "rU") as handle:
            reader = FastReader(handle, 'utf-8', ui=Mock())
            data = list(reader)
            assert len(data) == 3

    def test_csv_file_with_lf_slow(self, csv_file_with_lf):
        """ Slow and LF """
        with open(csv_file_with_lf, "rU") as handle:
            reader = SlowReader(handle, 'utf-8', ui=Mock())
            data = list(reader)
            assert len(data) == 3
