import pytest
from datarobot_batch_scoring.api_response_handlers import pred_api_v10, api_v1
from datarobot_batch_scoring.consts import Batch
from datarobot_batch_scoring.exceptions import UnexpectedKeptColumnCount


@pytest.fixture()
def parsed_pred_api_v10_predictions():
    return [
        {
            "predictionValues": [{"value": 1, "label": "readmitted"}],
            "prediction": 1,
            "rowId": 0
        },
        {
            "predictionValues": [{"value": 1, "label": "readmitted"}],
            "prediction": 1,
            "rowId": 1
        }]


@pytest.fixture()
def parsed_api_v1_predictions():
    return {
        'model_id': '123',
        'status': '',
        'code': 200,
        'execution_time': 100,
        'task': 'Binary',
        'version': 'v1',
        'predictions': [{'class_probabilities': {'0.0': 0.4,
                                                 '1.0': 0.6},
                         'prediction': 1.0,
                         'row_id': 0},
                        {'class_probabilities': {'0.0': 0.6,
                                                 '1.0': 0.4},
                         'prediction': 0.0,
                         'row_id': 1}]
    }


@pytest.fixture()
def batch():
    return Batch(id=0,
                 fieldnames=['race', 'gender', 'age', 'weight', 'readmitted'],
                 rows=2,
                 data=[
                     ['Caucasian', 'Male', '[50-60)', '?', 'FALSE'],
                     ['Caucasian', 'Male', '[50-60)', '?', 'TRUE']
                 ],
                 rty_cnt=3)


@pytest.fixture()
def fast_batch_quoted_newline():
    return Batch(id=0,
                 fieldnames=['race', 'gender', 'age', 'weight', 'readmitted'],
                 rows=2,
                 data=[
                     'Caucasian,Male,[50-60),?,FALSE',
                     'Caucasian,Male,[50-60),?',
                     'TRUE'
                 ],
                 rty_cnt=3)


@pytest.fixture()
def fast_batch_with_quoted_comma():
    return Batch(id=0,
                 fieldnames=['race', 'gender', 'age', 'weight', 'readmitted'],
                 rows=2,
                 data=[
                     'Caucasian,Male,[50-60),?,FALSE',
                     'Caucasian,Male,"[50,60)",?,TRUE'
                 ],
                 rty_cnt=3)


class TestPredApiV10Handlers(object):
    def test_unpack_data(self):
        result = pred_api_v10.unpack_data({
            'elapsed': 0.123,
            'text': '''
            {
                "data": [{
                    "predictionValues": [{"value":5.0001011968,"label":"z"}],
                    "prediction":5.0001011968,
                    "rowId":0
                },
                {
                    "predictionValues": [{"value":5.0001011968,"label":"z"}],
                    "prediction":5.0001011968,
                    "rowId":0
                }]
            }
            ''',
            'headers': {
                'X-DataRobot-Execution-Time': 10
            }
        })
        assert type(result) is tuple
        data, exec_time, elapsed = result
        assert len(data) == 2
        assert elapsed == 0.123
        assert exec_time == 10

    @pytest.mark.parametrize('opts, expected_fields, expected_values', (
        ({'pred_name': None,
          'pred_decision_name': None,
          'keep_cols': None,
          'skip_row_id': False,
          'fast_mode': False,
          'delimiter': ','},
         ['row_id', 'readmitted'],
         [[0, 1], [1, 1]]),

        ({'pred_name': None,
          'pred_decision_name': None,
          'keep_cols': ['gender'],
          'skip_row_id': False,
          'fast_mode': False,
          'delimiter': ','},
         ['row_id', 'gender', 'readmitted'],
         [[0, 'Male', 1], [1, 'Male', 1]]),

        ({'pred_name': None,
          'pred_decision_name': None,
          'keep_cols': ['gender'],
          'skip_row_id': True,
          'fast_mode': False,
          'delimiter': ','},
         ['gender', 'readmitted'],
         [['Male', 1], ['Male', 1]]),

        ({'pred_name': None,
          'pred_decision_name': 'label',
          'keep_cols': None,
          'skip_row_id': False,
          'fast_mode': False,
          'delimiter': ','},
         ['row_id', 'readmitted', 'label'],
         [[0, 1, 1], [1, 1, 1]]),
    ))
    def test_format_data(self, parsed_pred_api_v10_predictions, batch,
                         opts, expected_fields, expected_values):
        fields, values = pred_api_v10.format_data(
            parsed_pred_api_v10_predictions, batch, **opts)
        assert fields == expected_fields
        assert values == expected_values


class TestApiV1Handlers(object):
    def test_unpack_data(self):
        result = api_v1.unpack_data({
            'elapsed': 0.123,
            'text': '''
                {"status": "",
                 "model_id": "598b07d8100d2b4b0cd39224",
                 "code": 200,
                 "execution_time": 10,
                 "predictions": [
                     {
                        "row_id": 0,
                        "class_probabilities":{"0.0":0.4,"1.0":0.6},
                        "prediction":1.0
                    },
                    {
                        "row_id": 1,
                        "class_probabilities":{"0.0":0.4,"1.0":0.6},
                        "prediction":1.0
                    }
                ]}
            ''',
            'headers': {
                'X-DataRobot-Model-Cache-Hit': 'true'
            }
        })
        assert type(result) is tuple
        data, exec_time, elapsed = result
        assert type(data) is dict
        assert set(data.keys()) == {
            'status', 'model_id', 'code', 'execution_time', 'predictions'}
        assert elapsed == 0.123
        assert exec_time == 10

    @pytest.mark.parametrize('opts, expected_fields, expected_values', (
        ({'pred_name': None,
          'keep_cols': None,
          'skip_row_id': False,
          'fast_mode': False,
          'delimiter': ','},
         ['row_id', '0.0', '1.0'],
         [[0, 0.4, 0.6], [1, 0.6, 0.4]]),

        ({'pred_name': None,
          'keep_cols': ['gender'],
          'skip_row_id': False,
          'fast_mode': False,
          'delimiter': ','},
         ['row_id', 'gender', '0.0', '1.0'],
         [[0, 'Male', 0.4, 0.6], [1, 'Male', 0.6, 0.4]]),

        ({'pred_name': None,
          'keep_cols': ['gender'],
          'skip_row_id': True,
          'fast_mode': False,
          'delimiter': ','},
         ['gender', '0.0', '1.0'],
         [['Male', 0.4, 0.6], ['Male', 0.6, 0.4]]),
    ))
    def test_format_data(self, parsed_api_v1_predictions, batch,
                         opts, expected_fields, expected_values):
        fields, values = api_v1.format_data(
            parsed_api_v1_predictions, batch, **opts)
        assert fields == expected_fields
        assert values == expected_values

    @pytest.mark.parametrize('opts', [
        {'pred_name': None,
         'keep_cols': ['age'],
         'skip_row_id': False,
         'fast_mode': True,
         'delimiter': ','}
    ])
    def test_fail_on_quoted_newline_in_fast_mode(self,
                                                 parsed_api_v1_predictions,
                                                 fast_batch_quoted_newline,
                                                 opts):
        with pytest.raises(UnexpectedKeptColumnCount):
            api_v1.format_data(parsed_api_v1_predictions,
                               fast_batch_quoted_newline, **opts)

    @pytest.mark.parametrize('opts', [
        {'pred_name': None,
         'keep_cols': ['age'],
         'skip_row_id': False,
         'fast_mode': True,
         'delimiter': ','}
    ])
    def test_fail_on_quoted_comma_in_fast_mode(self, parsed_api_v1_predictions,
                                               fast_batch_with_quoted_comma,
                                               opts):
        with pytest.raises(UnexpectedKeptColumnCount):
            api_v1.format_data(parsed_api_v1_predictions,
                               fast_batch_with_quoted_comma, **opts)
