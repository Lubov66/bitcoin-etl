import hashlib
import logging

import requests

_session_cache = {}
retry_total = 5
logger = logging.getLogger(__name__)

def _get_session(endpoint_uri):
    cache_key = hashlib.md5(endpoint_uri.encode('utf-8')).hexdigest()
    if cache_key not in _session_cache:
        _session_cache[cache_key] = requests.Session()
    return _session_cache[cache_key]


def make_post_request(endpoint_uri, data, *args, **kwargs):
    for i in range(retry_total):
        try:
            kwargs.setdefault('timeout', 10)
            session = _get_session(endpoint_uri)
            response = session.post(endpoint_uri, data=data, *args, **kwargs)
            response.raise_for_status()
            return response.content
        except Exception as e:
            if retry_total == i + 1:
              raise e
            logger.warning(f"An error was encountered sending the RPC request {e}")
