import csv
import logging
import os
from urllib2 import urlopen

import pandas
from bs4 import BeautifulSoup
from retrying import retry

from taskpool import TaskPool

_HDB_URL = 'https://services2.hdb.gov.sg'


def load_flats():
    data_dir = 'data'
    flats_url = _HDB_URL + '/web/ej03/svy21kml/flats/%s'
    flats = ('flats-kml-%d.kml' % count for count in xrange(1, 858))
    flat_identifiers = list()
    for flat in flats:
        flat_path = os.path.sep.join([data_dir, flat])
        flat_path_empty = os.path.sep.join([data_dir, 'empty', flat])
        if not os.path.exists(flat_path) and not os.path.exists(flat_path_empty):
            logging.info('loading data for %s from web' % flat)
            xml_text = urlopen(flats_url % flat).read()
            with open(flat_path, 'w') as flat_file:
                flat_file.write(xml_text)

        else:
            logging.info('loading data for %s from cache' % flat)
            if os.path.exists(flat_path_empty):
                flat_path = flat_path_empty

            with open(flat_path, 'r') as flat_file:
                xml_text = flat_file.read()

        xml = BeautifulSoup(xml_text, 'xml')
        current_flat_identifiers = xml.find_all('key1')
        flat_identifiers += [(flat, tag.contents[0]) for tag in current_flat_identifiers]

    return flat_identifiers


def load_prop_info(prop_id):
    prop_info_url = _HDB_URL + '/webapp/BC16AWPropInfoXML/BC16SRetrievePropInfoXML?sysId=FI10&bldngGL=%s'
    xml_text = urlopen(prop_info_url % prop_id).read()
    xml = BeautifulSoup(xml_text, 'xml')
    block_tag = xml.find('Block')
    if not block_tag.contents:
        return None

    block = block_tag.contents[0].strip()
    street_name = xml.find('StreetName').contents[0].strip()
    postal_code = xml.find('PostalCode').contents[0].strip()

    return block, street_name, postal_code


def load_lease_info(postal_code):
    lease_info_url = _HDB_URL + '/webapp/BB14ALeaseInfo/BB14SGenerateLeaseInfoXML?postalCode=%s'
    xml_text = urlopen(lease_info_url % postal_code).read()
    xml = BeautifulSoup(xml_text, 'xml')
    lease_commenced_date = xml.find('LeaseCommencedDate').contents[0].strip()
    lease_remaining = xml.find('LeaseRemaining').contents[0].strip()
    lease_period = xml.find('LeasePeriod').contents[0].strip()
    return postal_code, lease_commenced_date, lease_remaining, lease_period


def load_resale(postal_code):
    resale_url = _HDB_URL + '/webapp/BB24RESLSUMMARY/BB24SResaleTransMapSummaryTable?postal=%s&fromDate=201506&toDate=201606'
    xml_text = urlopen(resale_url % postal_code).read()
    xml = BeautifulSoup(xml_text, 'xml')
    resale_data = list()
    for resale in xml.find_all('Dataset'):
        flat_type = ''
        if resale.find_next('flatType').contents:
            flat_type = resale.find_next('flatType').contents[0].strip()

        floor_area = ''
        if resale.find_next('floorArea').contents:
            floor_area = resale.find_next('floorArea').contents[0].strip()

        num_count = ''
        if resale.find_next('numCount').contents:
            num_count = resale.find_next('numCount').contents[0].strip()

        resale_price = ''
        if resale.find_next('resalePrice').contents:
            resale_price = resale.find_next('resalePrice').contents[0].strip()

        resale_data.append((flat_type, floor_area, num_count, resale_price))

    return resale_data


def generate_flats_db():

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_delay=30000)
    def process_building(count):
        flat_id = '{:05}'.format(count)
        logging.info('processing building %s' % flat_id)
        with open('flat-db2.csv', 'a') as flats_db:
            prop_info = load_prop_info(flat_id)
            if prop_info:
                line = ','.join([flat_id] + ['"%s"' % field for field in prop_info])
                flats_db.write(line + os.linesep)

            else:
                logging.info('no data found for building %s' % flat_id)

    mapper = TaskPool(pool_size=20)
    for count in xrange(1, 15288):
        mapper.add_task(process_building, count)

    logging.info('processing...')
    mapper.execute()
    logging.info('completed')


def process_resale():
    col_types = {'number': str, 'street': str, 'postal_code': str}
    buildings_df = pandas.read_csv('flat-db.csv', engine='c', dtype=col_types)
    postal_codes = buildings_df['postal_code'].unique()
    with open('resale-db.csv', 'a') as resale_db_file:
        field_names = ['postal_code', 'flat_type', 'floor_area', 'num_count', 'resale_price']
        writer = csv.DictWriter(resale_db_file, fieldnames=field_names)
        writer.writeheader()
        for postal_code in sorted(postal_codes):
            logging.info('processing postal code: %s' % postal_code)
            resale_data = load_resale(postal_code)
            for flat_type, floor_area, num_count, resale_price in resale_data:
                writer.writerow({
                    'postal_code': postal_code,
                    'flat_type': flat_type,
                    'floor_area': floor_area,
                    'num_count': num_count,
                    'resale_price': resale_price,
                })

            resale_db_file.flush()


@retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_delay=30000)
def load_residential_units(postal_code):
    url = _HDB_URL + '/webapp/BC16AWPropInfoXML/BC16SRetrieveResiUnitCountXML?systemID=BC16&programName=FI10&postalCode=%s'
    xml_text = urlopen(url % postal_code).read()
    xml = BeautifulSoup(xml_text, 'xml')
    unit_data = list()
    logging.info('processing postal code %s' % postal_code)
    for unit in xml.find_all('ResidentUnit'):
        room_type = ''
        room_count = 0
        if unit.find_next('actUseTypTxt').contents:
            room_type = unit.find_next('actUseTypTxt').contents[0].strip()

        if unit.find_next('count').contents:
            room_count = unit.find_next('count').contents[0].strip()

        unit_data.append((postal_code, room_type, room_count))

    return unit_data


def generate_units_data():
    col_types = {'number': str, 'street': str, 'postal_code': str}
    buildings_df = pandas.read_csv('flat-db.csv', engine='c', dtype=col_types)
    postal_codes = buildings_df['postal_code'].unique()
    with open('units-db.csv', 'w') as unit_file:
        field_names = ['postal_code', 'room_type', 'room_count']
        writer = csv.DictWriter(unit_file, fieldnames=field_names)
        writer.writeheader()
        mapper = TaskPool(pool_size=20)
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

        unit_file.close()


@retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_delay=30000)
def load_lease_data(postal_code):
    url = _HDB_URL + '/webapp/BB14ALeaseInfo/BB14SGenerateLeaseInfoXML?postalCode=%s'
    xml_text = urlopen(url % postal_code).read()
    xml = BeautifulSoup(xml_text, 'xml')
    logging.info('processing postal code %s' % postal_code)
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


def generate_lease_data():
    col_types = {'number': str, 'street': str, 'postal_code': str}
    buildings_df = pandas.read_csv('flat-db.csv', engine='c', dtype=col_types)
    postal_codes = buildings_df['postal_code'].unique()
    with open('lease-db.csv', 'w') as lease_file:
        field_names = ['postal_code', 'lease_commenced', 'lease_remaining', 'lease_period']
        writer = csv.DictWriter(lease_file, fieldnames=field_names)
        writer.writeheader()
        mapper = TaskPool(pool_size=20)
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


def main():
    rooms_df = pandas.read_csv('flat-db.csv')
    leases_df = pandas.read_csv('lease-db.csv')
    units_df = pandas.read_csv('units-db.csv')
    resales_df = pandas.read_csv('resale-db.csv')
    units_pivot_df = units_df.pivot(index='postal_code', columns='room_type', values='room_count')
    rooms_enhanced_df = rooms_df.join(units_pivot_df, on='postal_code')
    final_df = rooms_enhanced_df.join(leases_df.set_index('postal_code', inplace=False), on='postal_code')

    def make_address(row):
        return '{number} {street}'.format(number=row['number'], street=row['street'])

    final_df['Short Address'] = final_df.apply(make_address, axis=1)

    def extend_names(row):
        name = row['Short Address']
        replacements = {' Rd': ' Road',
                        ' Mkt': ' Market',
                        ' Cres': ' Crescent',
                        ' Dr': ' Drive',
                        ' Upp': ' Upper',
                        ' Pk': ' Park',
                        ' Cl': ' Close',
                        ' Hts': ' Heights',
                        ' Ctr': ' Center ',
                        ' Ave': ' Avenue',
                        "C'wealth": 'Commonwealth',
                        'Jln': 'Jalan',
                        'Kg': 'Kampong',
                        'Lor ': 'Lorong ',
                        ' Gdns': 'Gardens',
                        ' Sth': ' South',
                        ' Nth': ' North',
                        ' Ter': ' Terrace',
                        ' St': ' Street',
                        ' Sq': ' Square',
                        }
        for word, replacement in replacements.items():
            name = name.replace(word, replacement)

        return name

    #final_df['Address'] = final_df.apply(extend_names, axis=1)
    final_df['Postal Code'] = final_df['postal_code']
    final_df['Lease Year'] = final_df['lease_commenced']
    final_df['Lease Duration'] = final_df['lease_period']
    # re-ordering
    columns = ['Short Address', 'Postal Code', 'Lease Year', 'Lease Duration', '1-room', '2-room',
    '3-room', '4-room', '5-room', 'Executive', 'HUDC',
    'Multi-generation', 'Studio Apartment', 'Type S1', 'Type S2',
               ]
    export_df = final_df[columns].sort_values(by='Postal Code')
    export_df.to_csv('rooms-final.csv', index=False, float_format='%.f')

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    logging.info('started')
    main()
