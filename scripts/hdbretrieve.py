import logging

import pandas

from hdb.hdbdownload import generate_rooms_db, generate_units_db, generate_leases_db, set_data_dir, set_hdb_url
from hdb.urlcaching import set_cache_http


def generate_csv(data_dir, output_dir):
    buildings_df = pandas.read_csv(data_dir + 'buildings-db.csv')
    leases_df = pandas.read_csv(data_dir + 'leases-db.csv')
    units_df = pandas.read_csv(data_dir + 'units-db.csv')
    units_pivot_df = units_df.pivot(index='postal_code', columns='room_type', values='room_count')
    buildings_enhanced_df = buildings_df.join(units_pivot_df, on='postal_code')
    final_df = buildings_enhanced_df.join(leases_df.set_index('postal_code', inplace=False), on='postal_code')

    def make_address(row):
        return '{number} {street}'.format(number=row['number'], street=row['street'])

    final_df['Short Address'] = final_df.apply(make_address, axis=1)
    final_df['Postal Code'] = final_df['postal_code']
    final_df['Lease Year'] = final_df['lease_commenced']
    final_df['Lease Duration'] = final_df['lease_period']
    # re-ordering
    columns = ['Short Address', 'Postal Code', 'Lease Year', 'Lease Duration', '1-room', '2-room',
    '3-room', '4-room', '5-room', 'Executive', 'HUDC',
    'Multi-generation', 'Studio Apartment', 'Type S1', 'Type S2',
               ]
    export_df = final_df[columns].sort_values(by='Postal Code')
    export_df.to_csv(output_dir + 'rooms-final.csv', index=False, float_format='%.f')

if __name__ == '__main__':
    DATA_DIR = '.hdb/'
    HDB_URL = 'https://services2.hdb.gov.sg'
    NB_TASKS = 60
    set_data_dir(DATA_DIR)
    set_hdb_url(HDB_URL)
    set_cache_http(DATA_DIR + '.urlcaching')
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    logging.info('started')
    logging.info('generating buildings data')
    generate_rooms_db()
    logging.info('generating leases data')
    generate_leases_db()
    logging.info('generating units data')
    generate_units_db()
    logging.info('generating CSV')
    generate_csv(DATA_DIR, './')
