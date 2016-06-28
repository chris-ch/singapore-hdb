import requests
import requests_cache


def set_cache_http(cache_file_path):
    requests_cache.install_cache(cache_file_path)


def open_url(url):
    return requests.get(url).text
