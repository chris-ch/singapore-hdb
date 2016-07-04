import argparse
import logging

__version__ = '0.2'

from hdb.hdbdownload import generate_buildings_db, generate_units_db, generate_leases_db, set_data_dir, set_hdb_url, \
    set_pool_size, generate_excel
from hdb.urlcaching import set_cache_http


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.info('version %s', __version__)
    parser = argparse.ArgumentParser(description='Loading building data from HDB.',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter
                                     )
    parser.add_argument('output_file', type=str, nargs='?', help='name of the output Excel file', default='hdb.xlsx')
    parser.add_argument('--ntasks', type=int, help='number of simultaneous downloads', default=40)
    parser.add_argument('--use-cache', help='stores downloaded HDB files locally', action='store_true')
    parser.add_argument('--only-output', help='skips downloading steps (will fail if missing cache files)', action='store_true')
    parser.add_argument('--max-building-id', help='max building id to scan', default=15288)
    args = parser.parse_args()
    DATA_DIR = '.hdb/'
    HDB_URL = 'https://services2.hdb.gov.sg'
    set_data_dir(DATA_DIR)

    if not args.only_output:
        set_hdb_url(HDB_URL)
        set_pool_size(args.ntasks)
        if args.use_cache:
            set_cache_http('.urlcaching')

        logging.info('started')
        logging.info('generating buildings data')
        generate_buildings_db(args.max_building_id)
        logging.info('generating leases data')
        generate_leases_db()
        logging.info('generating units data')
        generate_units_db()

    logging.info('generating output')
    generate_excel(DATA_DIR, './', args.output_file)
