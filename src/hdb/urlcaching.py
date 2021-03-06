import logging
import os

import itertools
import threading

from datetime import datetime
import requests
import hashlib

_CACHE_FILE_PATH = None
_MAX_NODE_FILES = 0x400
_REBALANCING_LIMIT = 0x1000


_rebalancing = threading.Condition()


def set_cache_http(cache_file_path):
    global _CACHE_FILE_PATH
    cache_file_path_full = os.path.abspath(cache_file_path)
    _CACHE_FILE_PATH = cache_file_path_full
    if not os.path.exists(_CACHE_FILE_PATH):
        os.makedirs(_CACHE_FILE_PATH)

    logging.debug('setting cache path: %s', cache_file_path_full)


def is_cache_used():
    return _CACHE_FILE_PATH is not None


def _get_directories_under(path):
    return (node for node in os.listdir(path) if os.path.isdir(os.path.join(path, node)))


def _get_files_under(path):
    return (node for node in os.listdir(path) if os.path.isfile(os.path.join(path, node)))


def _generator_count(a_generator):
    return sum(1 for item in a_generator)


def _divide_node(path, nodes_path):
    level = len(nodes_path)
    new_node_sup_init = 'FF' * 20
    new_node_inf_init = '7F' + 'FF' * 19
    if level > 0:
        new_node_sup = nodes_path[-1]
        new_node_diff = (int(new_node_sup_init, 16) - int(new_node_inf_init, 16)) >> level
        new_node_inf = '%0.40X' % (int(new_node_sup, 16) - new_node_diff)

    else:
        new_node_sup = new_node_sup_init
        new_node_inf = new_node_inf_init

    new_path_1 = os.path.sep.join([path] + nodes_path + [new_node_inf.lower()])
    new_path_2 = os.path.sep.join([path] + nodes_path + [new_node_sup.lower()])
    return os.path.abspath(new_path_1), os.path.abspath(new_path_2)


def rebalance_cache_tree(path, nodes_path=None):
    if not nodes_path:
        nodes_path = list()

    current_path = os.path.sep.join([path] + nodes_path)
    files_node = _get_files_under(current_path)
    rebalancing_required = _generator_count(itertools.islice(files_node, _MAX_NODE_FILES + 1)) > _MAX_NODE_FILES
    if rebalancing_required:
        new_path_1, new_path_2 = _divide_node(path, nodes_path)
        logging.info('rebalancing required, creating nodes: %s and %s', os.path.abspath(new_path_1), os.path.abspath(new_path_2))
        _rebalancing.acquire()
        _rebalancing.wait()
        if not os.path.exists(new_path_1):
            os.makedirs(new_path_1)

        if not os.path.exists(new_path_2):
            os.makedirs(new_path_2)

        for filename in _get_files_under(current_path):
            file_path = os.path.sep.join([current_path, filename])
            if file_path <= new_path_1:
                logging.info('moving %s to %s', filename, new_path_1)
                os.rename(file_path, os.path.sep.join([new_path_1, filename]))

            else:
                logging.info('moving %s to %s', filename, new_path_2)
                os.rename(file_path, os.path.sep.join([new_path_2, filename]))

        _rebalancing.release()

    for directory in _get_directories_under(current_path):
        rebalance_cache_tree(path, nodes_path + [directory])


def find_node(digest, path=None):
    if not path:
        path = _CACHE_FILE_PATH

    directories = sorted(_get_directories_under(path))

    if not directories:
        return path

    else:
        target_directory = None
        for directory_name in directories:
            if digest <= directory_name:
                target_directory = directory_name
                break

        if not target_directory:
            raise Exception('Inconsistent cache tree')

        return find_node(digest, path=os.path.sep.join([path, target_directory]))


def get_cache_filename(key):
    hash_md5 = hashlib.md5()
    hash_md5.update(key.encode('utf-8'))
    digest = hash_md5.hexdigest()
    target_node = find_node(digest)
    cache_filename = os.sep.join([target_node, digest])
    return cache_filename


def is_cached(key):
    cache_filename = get_cache_filename(key)
    return os.path.exists(cache_filename)


def file_size(filename):
    count = -1
    with open(filename) as file_lines:
        for count, line in enumerate(file_lines):
            pass

    return count + 1


def _add_to_cache(key, value):
    _rebalancing.acquire()
    try:
        logging.debug('adding to cache: %s', key)
        filename = get_cache_filename(key)
        index_name = os.path.sep.join([_CACHE_FILE_PATH, 'index'])
        today = datetime.today().strftime('%Y%m%d')
        with open(filename, 'w') as cache_content:
            cache_content.write(value)

        with open(index_name, 'a') as index_file:
            filename_digest = filename.split(os.path.sep)[-1]
            index_file.write('%s %s: "%s"\n' % (today, filename_digest, key))

    finally:
        _rebalancing.notify_all()
        _rebalancing.release()

    if file_size(index_name) % _REBALANCING_LIMIT == 0:
        logging.debug('rebalancing cache')
        rebalance_cache_tree(_CACHE_FILE_PATH)


def _get_from_cache(key):
    _rebalancing.acquire()
    try:
        logging.debug('reading from cache: %s', key)
        with open(get_cache_filename(key), 'r') as cache_content:
            content = cache_content.read()

    finally:
        _rebalancing.notify_all()
        _rebalancing.release()

    return content


def open_url(url):
    logging.debug('opening url: %s', url)
    if is_cache_used():
        if not is_cached(url):
            content = requests.get(url).text
            _add_to_cache(url, content)

        content = _get_from_cache(url)

    else:
        # straight access
        content = requests.get(url).text

    return content
