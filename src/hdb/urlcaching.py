import logging
import os
import requests
import hashlib

_CACHE_FILE_PATH = None


def set_cache_http(cache_file_path):
    global _CACHE_FILE_PATH
    cache_file_path_full = os.path.abspath(cache_file_path)
    _CACHE_FILE_PATH = cache_file_path_full
    if not os.path.exists(_CACHE_FILE_PATH):
        os.makedirs(_CACHE_FILE_PATH)

    logging.debug('setting cache path: %s', cache_file_path_full)


def open_url(url):
    logging.debug('opening url: %s', url)
    if _CACHE_FILE_PATH:
        hash_md5 = hashlib.md5()
        hash_md5.update(url.encode('utf-8'))
        cache_filename = os.sep.join([_CACHE_FILE_PATH, hash_md5.hexdigest()])
        if not os.path.exists(cache_filename):
            content = requests.get(url).text
            with open(cache_filename, 'w') as cache_content:
                cache_content.write(content)

        with open(cache_filename ,'r') as cache_content:
            content = cache_content.read()

    else:
        # straight access
        content = requests.get(url).text

    return content
