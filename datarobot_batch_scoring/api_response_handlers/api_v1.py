import operator
import json

from datarobot_batch_scoring.consts import TargetType
from datarobot_batch_scoring.exceptions import UnexpectedKeptColumnCount


def format_data(result, batch, **opts):
    pred_name = opts.get('pred_name')
    keep_cols = opts.get('keep_cols')
    skip_row_id = opts.get('skip_row_id')
    fast_mode = opts.get('fast_mode')

    predictions = result['predictions']
    if result['task'] == TargetType.BINARY:
        sorted_classes = list(
            sorted(predictions[0]['class_probabilities'].keys()))
        out_fields = ['row_id'] + sorted_classes
        if pred_name is not None:
            sorted_classes = [sorted_classes[-1]]
            out_fields = ['row_id'] + [pred_name]
        pred = [[p['row_id'] + batch.id] +
                [p['class_probabilities'][c] for c in
                 sorted_classes]
                for p in
                sorted(predictions,
                       key=operator.itemgetter('row_id'))]
    elif result['task'] == TargetType.REGRESSION:
        pred = [[p['row_id'] + batch.id, p['prediction']]
                for p in
                sorted(predictions,
                       key=operator.itemgetter('row_id'))]
        out_fields = ['row_id', pred_name if pred_name else '']
    else:
        raise ValueError('task "{}" not supported'
                         ''.format(result['task']))

    if keep_cols:
        # stack columns

        feature_indices = {col: i for i, col in
                           enumerate(batch.fieldnames)}
        indices = [feature_indices[col] for col in keep_cols]

        written_fields = []

        if not skip_row_id:
            written_fields.append('row_id')

        written_fields += keep_cols + out_fields[1:]

        # first column is row_id
        comb = []
        for row, predicted in zip(batch.data, pred):
            if fast_mode:
                # row is a full line, we need to cut it into fields
                if len(row) != len(batch.fieldnames):
                    raise UnexpectedKeptColumnCount()
            keeps = [row[i] for i in indices]
            output_row = []
            if not skip_row_id:
                output_row.append(predicted[0])
            output_row += keeps + predicted[1:]
            comb.append(output_row)
    else:
        if not skip_row_id:
            comb = pred
            written_fields = out_fields
        else:
            written_fields = out_fields[1:]
            comb = [row[1:] for row in pred]

    return written_fields, comb


def unpack_data(request):
    result = json.loads(request['text'])
    exec_time = result['execution_time']
    elapsed_total_seconds = request['elapsed']
    return result, exec_time, elapsed_total_seconds
