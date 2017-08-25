from . import pred_api_v10, api_v1

PRED_API_V10 = 'predApi/v1.0'
API_V1 = 'api/v1'

RESPONSE_HANDLERS = {
    PRED_API_V10: (pred_api_v10.unpack_data, pred_api_v10.format_data),
    API_V1: (api_v1.unpack_data, api_v1.format_data),
}


def get_response_handlers_from_url(url):
    if PRED_API_V10 in url:
        return RESPONSE_HANDLERS[PRED_API_V10]
    elif API_V1 in url:
        return RESPONSE_HANDLERS[API_V1]

    raise ValueError('Failed to find a handler for url: {}'.format(url))
