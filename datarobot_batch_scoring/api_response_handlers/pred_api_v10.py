import operator
from itertools import chain
import json
from six.moves import zip

from datarobot_batch_scoring.exceptions import UnexpectedKeptColumnCount


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
    prediction_explanations_key
):
    """ Generate prediction explanations fields

    Parameters
    ----------
    result_sorted : list[dict]
        list of results sorted by rowId
    prediction_explanations_key : str
        key of results by which explanation fields can be found

    Returns
    -------
    header: list[str]
    row_generator: iterator
    """
    headers = []
    single_row = result_sorted[0]

    num_reason_codes = len(single_row[prediction_explanations_key])
    for num in range(1, num_reason_codes + 1):
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
            yield reason_codes

    return headers, rows_generator()


def format_data(result, batch, **opts):
    """ Generate rows of response from results

    Parameters
    ----------
    result : list
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
    # pred_decision = opts.get('pred_decision')
    keep_cols = opts.get('keep_cols')
    skip_row_id = opts.get('skip_row_id')
    fast_mode = opts.get('fast_mode')
    input_delimiter = opts.get('delimiter')

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
                prediction_explanations_key
            )
        )
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
