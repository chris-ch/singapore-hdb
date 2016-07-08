import logging

from hdb import urlcaching
from hdb.hdbdownload import generate_ethnic_db


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    logging.getLogger('requests').setLevel(logging.WARNING)
    file_handler = logging.FileHandler('hdb.log', mode='w')
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)
    urlcaching.set_cache_http('/Users/christophe/.ethnic')
    #postal_codes = ('090102', '090103', '090104')
    generate_ethnic_db()

if __name__ == '__main__':
    main()
