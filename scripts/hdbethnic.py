import argparse
import logging

from hdb import urlcaching
from hdb.hdbdownload import generate_ethnic_db, set_pool_size


def main():
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    logging.getLogger('requests').setLevel(logging.WARNING)
    file_handler = logging.FileHandler('hdb.log', mode='w')
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)
    urlcaching.set_cache_http('/Users/christophe/.ethnic')
    #postal_codes = ('090102', '090103', '090104')
    parser = argparse.ArgumentParser(description='Loading ethnic data from HDB.',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter
                                     )
    parser.add_argument('--ntasks', type=int, help='number of simultaneous downloads', default=40)
    args = parser.parse_args()
    set_pool_size(args.ntasks)
    generate_ethnic_db()

if __name__ == '__main__':
    main()
