import argparse
import logging

from hdb.hdbdownload import generate_rooms_db, generate_units_db, generate_leases_db, set_data_dir, set_hdb_url, \
    set_pool_size, generate_csv
from hdb.urlcaching import set_cache_http


def main():
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    logging.getLogger('requests').setLevel(logging.WARNING)
    parser = argparse.ArgumentParser(description='Loading building data from HDB.',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter
                                     )
    parser.add_argument('output_file', type=str, nargs='?', help='name of the output CSV file', default='output.csv')
    parser.add_argument('--ntasks', type=int, help='number of simultaneous downloads', default=10)
    args = parser.parse_args()
    DATA_DIR = '.hdb/'
    HDB_URL = 'https://services2.hdb.gov.sg'
    set_data_dir(DATA_DIR)
    set_hdb_url(HDB_URL)
    set_pool_size(args.ntasks)
    set_cache_http('.urlcaching')
    logging.info('started')
    logging.info('generating buildings data')
    generate_rooms_db()
    logging.info('generating leases data')
    generate_leases_db()
    logging.info('generating units data')
    generate_units_db()
    logging.info('generating CSV')
    generate_csv(DATA_DIR, './', args.output_file)

if __name__ == '__main__':
    main()
