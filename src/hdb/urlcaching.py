import logging
import os
import requests
import requests_cache


def set_cache_http(cache_file_path):
    cache_file_path_full = os.path.abspath(cache_file_path)
    logging.debug('setting cache path: %s', cache_file_path_full)
    requests_cache.install_cache(cache_file_path_full)


def open_url(url):
    logging.debug('opening url: %s', url)
    return requests.get(url).text
