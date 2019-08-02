import operator
from itertools import chain
import json
from six.moves import zip

from datarobot_batch_scoring.exceptions import UnexpectedKeptColumnCount, \
    NoPredictionThresholdInResult


def row_id_field(result_sorted, batch):
    """ Generate row_id field

    Parameters
    ----------
    result_sorted : list[dict]
        list of results sorted by rowId
    batch
        batch information

    Returns
    -------
    header: list[str]
    row_generator: iterator
    """
    return ['row_id'], (
        [record['rowId'] + batch.id]
        for record in result_sorted
    )


def prediction_values_fields(result_sorted, pred_name):
    """ Generate predictionValues fields

    Parameters
    ----------
    result_sorted : list[dict]
        list of results sorted by rowId
    pred_name: str, None
        column name for prediction results (may be None)

    Returns
    -------
    header: list[str]
    row_generator: iterator
    """
    single_row = result_sorted[0]
    fields = sorted(
        record['label']
        for record in single_row['predictionValues']
    )
    if pred_name is not None:
        headers = [pred_name]
        # batch scoring returns only positive class for binary tasks if
        # user specified pred_name option, so we eliminate negative
        # result
        if len(fields) == 2 and single_row['prediction'] in fields:
            fields = [fields[-1]]
    else:
        headers = fields

    rows_generator = (
        [
            pred_vals['value']
            for pred_vals in
            sorted(
                record['predictionValues'],
                key=operator.itemgetter('label')
            )
            if pred_vals['label'] in fields
        ] for record in result_sorted
    )

    return headers, rows_generator


def keep_original_fields(batch, keep_cols, fast_mode, input_delimiter):
    """ Generate fields from original data

    Parameters
    ----------
    batch
        batch of original data
    keep_cols: list[str]
        columns we should keep
    fast_mode: bool
        faster CSV processor. if True then row in the batch is raw string
    input_delimiter: str, optional
        csv delimiter. should be passed if fast_mode is True

    Returns
    -------
    header: list[str]
    row_generator: iterator
    """

    feature_indices = {col: i for i, col in
                       enumerate(batch.fieldnames)}
    indices = [feature_indices[col] for col in keep_cols]

    def rows_generator():
        for original_row in batch.data:
            if fast_mode:
                # row is a full line, we need to cut it into fields
                original_row = original_row.rstrip().split(input_delimiter)
                if len(original_row) != len(batch.fieldnames):
                    raise UnexpectedKeptColumnCount()

            yield [original_row[i] for i in indices]

    return keep_cols, rows_generator()


def prediction_explanation_fields(
    result_sorted,
    prediction_explanations_key,
    num_prediction_explanations,
):
    """ Generate prediction explanations fields

    Parameters
    ----------
    result_sorted : list[dict]
        list of results sorted by rowId
    prediction_explanations_key : str
        key of results by which explanation fields can be found
    num_prediction_explanations : int
        the total number of prediction explanations in response.
        Please note that in case output contains less explanations
        than this limit, the rest of the columns in the output will be pad
        with empty values for the sake of fixed columns count in the response.

    Returns
    -------
    header: list[str]
    row_generator: iterator
    """
    headers = []

    for num in range(1, num_prediction_explanations + 1):
        headers += [
            'explanation_{0}_feature'.format(num),
            'explanation_{0}_strength'.format(num)
        ]

    def rows_generator():
        for in_row in result_sorted:
            reason_codes = []
            for raw_reason_code in in_row[prediction_explanations_key]:
                reason_codes += [
                    raw_reason_code['feature'],
                    raw_reason_code['strength']
                ]
            # pad response with empty values in case not all prediction
            # explanations are present
            reason_codes += [''] * (len(headers) - len(reason_codes))
            yield reason_codes

    return headers, rows_generator()


def pred_threshold_field(result_sorted, pred_threshold_name):
    """ Generate prediction threshold field (for classification)
    Parameters
    ----------
    result_sorted : list[dict]
        list of results sorted by rowId
    pred_threshold_name : str
        column name which should contain prediction threshold
    Returns
    -------
    header: list[str]
    row_generator: iterator
    """

    return [pred_threshold_name], (
        [row.get('predictionThreshold')]
        for row in result_sorted
    )


def pred_decision_field(result_sorted, pred_decision):
    """ Generate prediction decision field

    Parameters
    ----------
    result_sorted : list[dict]
        list of results sorted by rowId
    pred_decision : str
        column name which should contain prediction decision (label)

    Returns
    -------
    header: list[str]
    row_generator: iterator
    """

    return [pred_decision], (
        [row.get('prediction')]
        for row in result_sorted
    )


def format_data(result, batch, **opts):
    """ Generate rows of response from results

    Parameters
    ----------
    result : list[dict]
        list of results
    batch
        batch information
    **opts
        additional arguments

    Returns
    -------
    written_fields : list
        headers
    comb : list
        list of rows
    """
    pred_name = opts.get('pred_name')
    pred_threshold_name = opts.get('pred_threshold_name')
    pred_decision_name = opts.get('pred_decision_name')
    keep_cols = opts.get('keep_cols')
    skip_row_id = opts.get('skip_row_id')
    fast_mode = opts.get('fast_mode')
    input_delimiter = opts.get('delimiter')
    max_prediction_explanations = opts.get('max_prediction_explanations')

    result_sorted = sorted(
        result,
        key=operator.itemgetter('rowId')
    )

    fields = []

    # region Columns

    # Row ID
    if not skip_row_id:
        fields.append(row_id_field(result_sorted, batch))

    # Kept columns from original data
    if keep_cols:
        fields.append(
            keep_original_fields(
                batch, keep_cols, fast_mode, input_delimiter
            )
        )

    # Prediction value columns
    fields.append(prediction_values_fields(result_sorted, pred_name))

    # Explanations columns
    single_row = result_sorted[0]
    prediction_explanations_keys = ('predictionExplanations', 'reasonCodes',
                                    None)

    def _find_prediction_explanations_key():
        return [key for key in prediction_explanations_keys
                if key in single_row or key is None][0]

    prediction_explanations_key = _find_prediction_explanations_key()
    if prediction_explanations_key:
        fields.append(
            prediction_explanation_fields(
                result_sorted,
                prediction_explanations_key,
                max_prediction_explanations,
            )
        )

    # Threshold field for classification
    # ('predictionThreshold' value from result)
    if pred_threshold_name:
        if 'predictionThreshold' in single_row:
            fields.append(
                pred_threshold_field(result_sorted, pred_threshold_name)
            )
        else:
            raise NoPredictionThresholdInResult()

    # Thresholded decision field ('prediction' value from result)
    if pred_decision_name:
        fields.append(pred_decision_field(result_sorted, pred_decision_name))

    # endregion

    headers = list(
        chain(*(header for header, _ in fields))
    )
    rows = list(
        list(chain(*row)) for row in
        zip(*(rows_gen for _, rows_gen in fields))
    )

    return headers, rows


def unpack_data(request):
    exec_time = float(request['headers']['X-DataRobot-Execution-Time'])
    result = json.loads(request['text'])  # replace with r.content
    elapsed_total_seconds = request['elapsed']
    return result['data'], exec_time, elapsed_total_seconds
