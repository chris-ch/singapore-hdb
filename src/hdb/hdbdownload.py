import csv
import logging
import os

import pandas
from bs4 import BeautifulSoup
from retrying import retry

from hdb.taskpool import TaskPool
from hdb.urlcaching import open_url

_HDB_URL = 'https://services2.hdb.gov.sg'
_DATA_DIR = '.hdb/'
_POOL_SIZE = 10
_MAX_BUILDING_ID=15288


def set_pool_size(pool_size):
    global _POOL_SIZE
    _POOL_SIZE = pool_size


def set_hdb_url(hdb_url):
    global _HDB_URL
    _HDB_URL = hdb_url


def set_data_dir(data_dir):
    global _DATA_DIR
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    _DATA_DIR = data_dir


@retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_delay=30000)
def load_prop_info(prop_id):
    prop_info_url = _HDB_URL + '/webapp/BC16AWPropInfoXML/BC16SRetrievePropInfoXML?sysId=FI10&bldngGL=%s'
    xml_text = open_url(prop_info_url % prop_id)
    xml = BeautifulSoup(xml_text, 'xml')
    block_tag = xml.find('Block')
    if not block_tag or not block_tag.contents:
        return None

    block = block_tag.contents[0].strip()
    street_name = xml.find('StreetName').contents[0].strip()
    postal_code = xml.find('PostalCode').contents[0].strip()

    return block, street_name, postal_code


@retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_delay=30000)
def load_residential_units(postal_code):
    url = _HDB_URL + '/webapp/BC16AWPropInfoXML/BC16SRetrieveResiUnitCountXML?systemID=BC16&programName=FI10&postalCode=%s'
    xml_text = open_url(url % postal_code)
    xml = BeautifulSoup(xml_text, 'xml')
    unit_data = list()
    logging.info('processing units data for postal code %s' % postal_code)
    for unit in xml.find_all('ResidentUnit'):
        room_type = ''
        room_count = 0
        if unit.find_next('actUseTypTxt').contents:
            room_type = unit.find_next('actUseTypTxt').contents[0].strip()

        if unit.find_next('count').contents:
            room_count = unit.find_next('count').contents[0].strip()

        unit_data.append((postal_code, room_type, room_count))

    return unit_data


@retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_delay=30000)
def load_lease_data(postal_code):
    url = _HDB_URL + '/webapp/BB14ALeaseInfo/BB14SGenerateLeaseInfoXML?postalCode=%s'
    xml_text = open_url(url % postal_code)
    xml = BeautifulSoup(xml_text, 'xml')
    logging.info('processing lease data for postal code %s' % postal_code)
    lease_info = xml.find('LeaseInformation')

    lease_commenced = ''
    if lease_info.find('LeaseCommencedDate'):
        lease_commenced = lease_info.find_next('LeaseCommencedDate').contents[0].strip()

    lease_remaining = ''
    if lease_info.find('LeaseRemaining'):
        lease_remaining = lease_info.find_next('LeaseRemaining').contents[0].strip()

    lease_period = ''
    if lease_info.find('LeasePeriod'):
        lease_period = lease_info.find_next('LeasePeriod').contents[0].strip()

    return postal_code, lease_commenced, lease_remaining, lease_period


def generate_buildings_db():

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_delay=30000)
    def process_building(building_id):
        building_id_formatted = '{:05}'.format(building_id)
        logging.info('processing data for building %s' % building_id_formatted)
        prop_info = load_prop_info(building_id_formatted)
        if not prop_info:
            logging.info('no data found for building %s' % building_id_formatted)
            return None

        return [building_id_formatted] + list(prop_info)

    mapper = TaskPool(pool_size=_POOL_SIZE)
    for count in xrange(1, _MAX_BUILDING_ID):
        mapper.add_task(process_building, count)

    logging.info('processing...')
    results = mapper.execute()
    logging.info('completed')

    with open(_DATA_DIR + 'buildings-db.csv', 'w') as rooms_file:
        field_names = ['building', 'number', 'street', 'postal_code']
        writer = csv.DictWriter(rooms_file, fieldnames=field_names)
        writer.writeheader()
        for result in results:
            if result is None:
                continue

            building, number, street, postal_code = result
            writer.writerow({
                'building': building,
                'number': number,
                'street': street,
                'postal_code': postal_code,
            })


def generate_units_db():
    col_types = {'number': str, 'street': str, 'postal_code': str}
    buildings_df = pandas.read_csv(_DATA_DIR + 'buildings-db.csv', engine='c', dtype=col_types)
    postal_codes = buildings_df['postal_code'].unique()
    with open(_DATA_DIR + 'units-db.csv', 'w') as units_file:
        field_names = ['postal_code', 'room_type', 'room_count']
        writer = csv.DictWriter(units_file, fieldnames=field_names)
        writer.writeheader()
        mapper = TaskPool(pool_size=_POOL_SIZE)
        for postal_code in sorted(postal_codes):
            logging.info('queuing postal code: %s' % postal_code)
            mapper.add_task(load_residential_units, postal_code)

        logging.info('processing...')
        results = mapper.execute()

        for dataset in results:
            for postal_code, room_type, room_count in dataset:
                writer.writerow({
                        'postal_code': postal_code,
                        'room_type': room_type,
                        'room_count': room_count
                })

        units_file.close()


def generate_leases_db():
    col_types = {'number': str, 'street': str, 'postal_code': str}
    buildings_df = pandas.read_csv(_DATA_DIR + 'buildings-db.csv', engine='c', dtype=col_types)
    postal_codes = buildings_df['postal_code'].unique()
    with open(_DATA_DIR + 'leases-db.csv', 'w') as lease_file:
        field_names = ['postal_code', 'lease_commenced', 'lease_remaining', 'lease_period']
        writer = csv.DictWriter(lease_file, fieldnames=field_names)
        writer.writeheader()
        mapper = TaskPool(pool_size=_POOL_SIZE)
        for postal_code in sorted(postal_codes):
            logging.info('queuing postal code: %s' % postal_code)
            mapper.add_task(load_lease_data, postal_code)

        logging.info('processing...')
        results = mapper.execute()

        for postal_code, lease_commenced, lease_remaining, lease_period in results:
            writer.writerow({
                        'postal_code': postal_code,
                        'lease_commenced': lease_commenced,
                        'lease_remaining': lease_remaining,
                        'lease_period': lease_period,
            })

        lease_file.close()


def generate_csv(data_dir, output_dir, output_file):
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
    export_df.to_csv(output_dir + output_file, index=False, float_format='%.f')
    full_path = os.path.abspath(output_dir + output_file)
    logging.info('file saved under %s', full_path)