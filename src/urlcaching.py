import shelve
import urllib2

_cache_file_path = '.urlcache.shelve'


def set_cache_file(cache_file_path):
    global _cache_file_path
    _cache_file_path = cache_file_path


def open_url(url):
    url_cache = shelve.open(_cache_file_path)
    if url not in url_cache:
        url_cache[url] = urllib2.urlopen(url).read()

    output = url_cache[url]
    url_cache.close()
    return output

