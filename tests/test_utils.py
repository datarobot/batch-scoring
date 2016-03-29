import pytest
from datarobot_batch_scoring.utils import verify_objectid


def test_invalid_objectid():
    with pytest.raises(ValueError):
        verify_objectid('123')
