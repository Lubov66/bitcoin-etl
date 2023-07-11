import hashlib

import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry

_session_cache = {}


def _get_session(endpoint_uri):
    cache_key = hashlib.md5(endpoint_uri.encode('utf-8')).hexdigest()
    if cache_key not in _session_cache:
        endpoint_session = requests.Session()
        retries = Retry(total=5, backoff_factor=0.1, status_forcelist=[429, 500, 502, 503, 504])

        endpoint_session.mount('http://', HTTPAdapter(max_retries=retries))
        endpoint_session.mount('https://', HTTPAdapter(max_retries=retries))

        _session_cache[cache_key] = endpoint_session
    return _session_cache[cache_key]


def make_post_request(endpoint_uri, data, *args, **kwargs):
    kwargs.setdefault('timeout', 10)
    session = _get_session(endpoint_uri)
    response = session.post(endpoint_uri, data=data, *args, **kwargs)
    response.raise_for_status()

    return response.content
