import requests
import datetime
import configparser
import argparse
import os
import sys
from datetime import datetime, timedelta
from peewee import *
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from time import strftime, sleep
import schedule



db = MySQLDatabase(None)  # will be initialized later


class BaseModel(Model):
    """A base model that will use our Sqlite database."""

    class Meta:
        database = db


class SolarEdge(BaseModel):
    ts = DateTimeField()
    ts_epoch = TimestampField()
    energy = SmallIntegerField()
    id = IntegerField(primary_key=True)


def config_section_map(conf, section):
    dict1 = {}
    options = conf.options(section)
    for option in options:
        try:
            dict1[option] = conf.get(section, option)
            if dict1[option] == -1:
                print("skip: %s" % option)
        except:
            print("exception on %s!" % option)
            dict1[option] = None
    return dict1


def parse_args() -> object:
    parser = argparse.ArgumentParser(description='Reads values from multiple API and writes it to MQTT and DB')
    parser.add_argument('-f', help='path and filename of the config file, default is ./config.rc',
                        default='config.rc')
#    parser.add_argument('-s', help="get solar edge data")
    parser.add_argument('-d', help='write the data also to MariaDB/MySQL DB', action='store_true', dest='db_write')

    return parser.parse_args()


def read_config(conf, config_file):
    try:
        c_db = config_section_map(conf, "DB")
    except:
        print("Could not find the DB conf section")
        config_full_path = os.getcwd() + "/" + config_file
        print("Tried to open the conf file: ", config_full_path)
        raise ValueError
    try:
        c_solar_edge = config_section_map(conf, "SOLAR_EDGE")
    except:
        print("Could not find the SOLAR_EDGE conf section")
        config_full_path = os.getcwd() + "/" + config_file
        print("Tried to open the conf file: ", config_full_path)
        raise ValueError
    try:
        c_influx = config_section_map(conf, "InfluxDB")
    except:
        print("Could not find the InfluxDB conf section")
        config_full_path = os.getcwd() + "/" + config_file
        print("Tried to open the conf file: ", config_full_path)
        raise ValueError
    return (c_db, c_solar_edge, c_influx)



def api_get_solaredge(conf):
    now = datetime.today()
    now_h1 = now - timedelta(days=0, hours=2)
    headers = {'cache-control': 'no-cache'}
    payload = {"timeUnit": "QUARTER_OF_AN_HOUR", "meters": "Production", "api_key": conf['token'],
               'endTime': now.strftime('%Y-%m-%d %H:%M:%S'), 'startTime': now_h1.strftime('%Y-%m-%d %H:%M:%S')}
#    payload['startTime'] = '2020-04-01 00:00:00'
#    payload['endTime'] = '2020-05-01 10:00:00'
    url = conf['url']

    try:
        response = requests.request("GET", url, data='', headers=headers, params=payload)
    except:
        print("Could not connect to the SolarEdge Cloud Server. Aborting")
        return None
    if response.status_code == 400:
        print("problem contacting the cloud")
        return None
    if response.status_code == 403:
        print("Error in the datetime arguments")
    return response.json()


def write2InfluxDB_energy(conf, dt_energy, energy_value):
    with InfluxDBClient(conf['url'], token=conf['token'], org=conf['org']) as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)

        ts = dt_energy
        p = Point(conf['measurement']).time(ts).field("energy", int(energy_value)).tag("location", conf['location'])
        write_api.write(bucket=conf['bucket'], org=conf['org'], record=p)


def job(conf_solaredge, conf_influx, db_write):
    solaredge_json = api_get_solaredge(conf_solaredge)
    if solaredge_json is None:
        exit(1)
    print("Data from solar edge:")
    for quarter in solaredge_json['energyDetails']['meters'][0]['values']:
        if 'value' in quarter:
            value = int(quarter['value'])
        else:
            value = 0
        print(quarter['date'], " ", value)
        datetime_object = datetime.strptime(quarter['date'], '%Y-%m-%d %H:%M:%S')
        ep = int(datetime_object.timestamp())

        if db_write:
            found_element_sel = SolarEdge.select().where(SolarEdge.ts_epoch == ep)
            try:
                found_element = found_element_sel.get()
            except:
                SolarEdge.insert(energy=value, ts=datetime_object, ts_epoch=ep).execute()
            else:
                ret = found_element.update(energy=value).where(SolarEdge.ts_epoch == ep).execute()
        write2InfluxDB_energy(conf_influx, datetime_object, value)


def main():
    args = parse_args()
    config = configparser.ConfigParser()
    config.read(args.f)
    try:
        (conf_db, conf_solaredge, conf_influx) = read_config(config, args.f)
    except ValueError:
        exit(1)
    if args.db_write:
        db.init(conf_db['db'], host=conf_db['host'], user=conf_db['username'], password=conf_db['password'],
                port=int(conf_db['port']))
        db.connect(conf_db)

    try:
        periodicity = int(conf_solaredge['periodicity'])
    except:
        sys.exit("Periodicity value must be int")

    job(conf_solaredge=conf_solaredge, conf_influx=conf_influx, db_write=args.db_write) # execute once before waiting
    schedule.every(periodicity).seconds.do(job, conf_solaredge=conf_solaredge, conf_influx=conf_influx, db_write=args.db_write)
    while True:
        schedule.run_pending()
        sleep(5)



# this is the standard boilerplate that calls the main() function
if __name__ == '__main__':
    main()
