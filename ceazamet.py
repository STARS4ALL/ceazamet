#!/usr/bin/python
# -*- coding: utf-8 -*-

import requests
from lxml import etree
import os
import csv
import copy
import json
import time
import urllib
import datetime
import argparse
import threading
import pandas as pd
from pytz import timezone
# from tzwhere import tzwhere
from influxdb import InfluxDBClient
from influxdb.client import InfluxDBClientError

INFLUXDB_HOST = os.environ.get('CEAZAMET_INFLUXDB_HOST', 'localhost')
INFLUXDB_PORT = os.environ.get('CEAZAMET_INFLUXDB_PORT', '8086')
INFLUXDB_USER = os.environ.get('CEAZAMET_INFLUXDB_USER', 'root')
INFLUXDB_PASSWORD = os.environ.get('CEAZAMET_INFLUXDB_PASSWORD', 'root')
INFLUXDB_DBNAME = os.environ.get('CEAZAMET_INFLUXDB_DBNAME', 'ceazamet')

CEAZAMET_URL = 'http://www.ceazamet.cl'
CEAZAMET_WS_1 = '/ws/pop_ws.php'
CEAZAMET_WS_2 = '/ws/davis/get_datos_scod.php'
CEAZAMET_NETWORK_STATE_WS = '/ws/davis/estado_red_cmet.php'

CEAZAMET_TIMEZONE = "America/Santiago"

CEAZAMET_USER = os.environ.get('CEAZAMET_USER', 'anon@nohost.com')

SLEEP_TIME = 3 * 60
FORCE_RELOAD = False

influxdb_client = None

sensors_to_register = [
    {'tf_nombre': u'Temperatura del Aire'.encode(encoding='UTF-8', errors='strict'), 'code': 'sensor_ta'},
    {'tf_nombre': u'Velocidad de Viento'.encode(encoding='UTF-8', errors='strict'), 'code': 'sensor_vv'},
    {'tf_nombre': u'Radiación Solar'.encode(encoding='UTF-8', errors='strict'), 'code': 'sensor_rs'},
    {'tf_nombre': u'Radiación Solar Difusa'.encode(encoding='UTF-8', errors='strict'), 'code': 'sensor_rs_dif'},
    {'tf_nombre': u'Radiación Solar Directa'.encode(encoding='UTF-8', errors='strict'), 'code': 'sensor_rs_dir'},
    {'tf_nombre': u'Radiación Solar Reflejada'.encode(encoding='UTF-8', errors='strict'), 'code': 'sensor_rs_ref'},
    {'tf_nombre': u'Presión Atmosférica'.encode(encoding='UTF-8', errors='strict'), 'code': 'sensor_pa'}
]

stations_by_minute = []


def __generateURL(ws, fn, options={}):
    _url = CEAZAMET_URL + ws + '?fn=' + fn

    if CEAZAMET_USER:
        options['user'] = CEAZAMET_USER

    _url += '&' + urllib.urlencode(options)

    # print(_url)

    return _url


def __getDataFrame(ws, fn, options={}, header=0):
    df = pd.read_csv(__generateURL(ws, fn, options), header=header)
    if header is not None:
        df.columns = df.columns.str.replace("#", "")
    return df

#   Get project stations
#
#   Params:
#       + p_cod : [ceazamet|changolab]
#       - e_owner : caeza
#       - e_cod : station code
#       - c0,...,c6 : [e_lat, e_lon, e_altitud, e_ultima_lectura, e_cod, e_nombre, e_primera_lectura, e_cod_provincia]
#       - geo : [1|0]


def requestAllStations(options={}):
    if 'p_cod' not in options:
        return {'error': 'p_cod option is obligatory'}

    options['encabezado'] = 1
    return __getDataFrame(CEAZAMET_WS_1, 'GetListaEstaciones', options)

#   Get station sensors
#
#   Params:
#       + p_cod : ceazamet
#       + e_cod : station code
#       - tm_cod : [ta_c|hr|vv_ms|dv|rs_w|pp_mm]
#       - c0,...,c9 : [e_cod, s_cod, tf_nombre, um_notacion,s_altura, s_ultima_lectura, tm_cod, s_primera_lectura]


def requestStationSensors(options={}):
    if 'p_cod' not in options:
        return {'error': 'p_cod option is obligatory'}

    if 'e_cod' not in options:
        return {'error': 'e_cod option is obligatory'}

    options['encabezado'] = 1
    df = __getDataFrame(CEAZAMET_WS_1, 'GetListaSensores', options, None)
    df.rename(
        columns={
            0: 'e_cod',
            1: 's_cod',
            2: 'tf_nombre',
            3: 'um_notacion',
            4: 's_altura',
            5: 's_ultima_lectura'
        },
        inplace=True
    )
    return df


#   Get sensor data
#
#   Params:
#       + s_cod : ceazamet
#       + fecha_inicio : [YYYY-MM-DD|YYYY-MM-DD HH:MM:SS]
#       + fecha_fin : [YYYY-MM-DD|YYYY-MM-DD HH:MM:SS]
#       - interv : [*hora|dia|mes]
#       - valor_nan : [(nada)*|NAN|9999|0|-1, cualquie valor texto o numerico]
#       - formato_nro :	[nro_bytes_enteros.nro_bytes_decimales]
#       - encabezado : [1(*)|0]
#       - tipo_resp : [(nada)*|JSON]


def requestSensorData(options={}):
    if 's_cod' not in options:
        return {'error': 's_cod option is obligatory'}

    if 'fecha_inicio' not in options:
        return {'error': 'fecha_inicio option is obligatory'}

    if 'fecha_fin' not in options:
        return {'error': 'fecha_fin option is obligatory'}

    options['encabezado'] = 1
    df = __getDataFrame(CEAZAMET_WS_1, 'GetSerieSensor', options, None)
    df.rename(
        columns={
            0: 's_cod',
            1: 'ultima_lectura',
            2: 'min',
            3: 'prom',
            4: 'max',
            5: 'data_pc'
        },
        inplace=True
    )
    return df


def requestSensorDataV2(options={}):
    if 'node_id' not in options:
        return {'error': 'node_id option is obligatory'}

    if 's_cod' not in options:
        return {'error': 's_cod option is obligatory'}

    if 'fecha_inicio' not in options:
        return {'error': 'fecha_inicio option is obligatory'}

    if 'fecha_fin' not in options:
        return {'error': 'fecha_fin option is obligatory'}

    try:
        df = pd.read_csv(__generateURL(CEAZAMET_WS_2, '', options), comment='#', header=None)
        df.rename(
            columns={
                0: 's_cod',
                1: 'datetime',
                2: 'min',
                3: 'prom',
                4: 'max'
            },
            inplace=True
        )
        return df
    except Exception as e:
        print(e)
        return pd.DataFrame()


def loadStationSensors(force=False):
    filename = 'stations-ceazamet'
    exists = os.path.isfile(filename)
    if exists and not force:
        with open(filename, 'r') as handle:
            return json.load(handle)
    else:
        station_sensors = []
        df_all_stations = requestAllStations({'p_cod': 'ceazamet', 'e_owner': 'ceaza'})

        if df_all_stations[df_all_stations['e_cod'].str.contains('PTN')].empty:
            df_all_stations = df_all_stations.append({
                "e_cod": "PTN",
                "e_nombre": "Patron",
                "e_lat": -30.00000,
                "e_lon": -70.00000,
                "e_altitud": 0
            }, ignore_index=True)

        for index_stations, row_stations in df_all_stations.iterrows():
            df_station_sensors = requestStationSensors({'p_cod': 'ceazamet', 'e_cod': row_stations['e_cod']})
            # timezone = tzwhere.tzwhere().tzNameAt(row_stations['e_lat'], row_stations['e_lon']) or "America/Santiago"
            timezone = CEAZAMET_TIMEZONE
            for index_sensor, row_sensor in df_station_sensors.iterrows():
                s = [x for x in sensors_to_register if x['tf_nombre'] == row_sensor['tf_nombre']]
                if s:
                    station_sensors.append({
                        'e_lat': row_stations['e_lat'],
                        'e_lon': row_stations['e_lon'],
                        'e_altitud': row_stations['e_altitud'],
                        'e_cod': row_stations['e_cod'],
                        'e_nombre': row_stations['e_nombre'],
                        'e_cod_provincia': row_stations['e_cod_provincia'],
                        's_cod': row_sensor['s_cod'],
                        'tf_nombre': row_sensor['tf_nombre'],
                        'um_notacion': row_sensor['um_notacion'],
                        's_altura': row_sensor['s_altura'],
                        'code': s[0]['code'],
                        'timezone': timezone
                    })

        with open(filename, 'w') as outfile:
            json.dump(station_sensors, outfile)
        return station_sensors


def sendSensorData(obj_sensor_info, df_sensor_data):
    try:
        print(df_sensor_data)
        if (df_sensor_data['min'].values[0] and df_sensor_data['prom'].values[0] and df_sensor_data['max'].values[0]):
            influxdb_points = [
                {
                    "measurement": "ceazamet",
                    "tags": obj_sensor_info,
                    "fields": {
                        "min": float(df_sensor_data['min'].values[0]),
                        "prom": float(df_sensor_data['prom'].values[0]),
                        "max": float(df_sensor_data['max'].values[0])
                    }
                }
            ]
            print(influxdb_points)
            influxdb_client.write_points(influxdb_points)
    except Exception as e:
        print(e)
        pass


def sendSensorDataV2(obj_sensor_info, df_sensor_data):
    try:
        influxdb_points = []
        for index_data, row_data in df_sensor_data.iterrows():
            try:
                influxdb_points.append({
                    "measurement": "ceazamet",
                    "tags": obj_sensor_info,
                    "time": __convertDate(row_data['datetime']).strftime('%Y-%m-%dT%H:%M:%SZ'),
                    "fields": {
                        "min": float(row_data['min']),
                        "prom": float(row_data['prom']),
                        "max": float(row_data['max'])
                    }
                })
            except Exception as e:
                print(e)
        if influxdb_points:
            print(influxdb_points)
            influxdb_client.write_points(influxdb_points)
    except Exception as e:
        print(e)
        pass


def __getSensorDataByMinute(sensor):
    fecha_inicio = datetime.datetime.now(timezone(CEAZAMET_TIMEZONE)) - datetime.timedelta(minutes=50)
    fecha_fin = datetime.datetime.now(timezone(CEAZAMET_TIMEZONE)) + datetime.timedelta(minutes=10)

    return requestSensorDataV2({
        'node_id': 'cmet_' + str(sensor['e_cod']),
        's_cod': sensor['s_cod'],
        'fecha_inicio': fecha_inicio.strftime("%Y-%m-%d %H:%M:%S"),
        'fecha_fin': fecha_fin.strftime("%Y-%m-%d %H:%M:%S")
    })


def __getSensorDataByHour(sensor):
    fecha_inicio = datetime.datetime.now(timezone(CEAZAMET_TIMEZONE)) - datetime.timedelta(minutes=59)
    fecha_fin = datetime.datetime.now(timezone(CEAZAMET_TIMEZONE))
    return requestSensorData({
        's_cod': sensor['s_cod'],
        'fecha_inicio': fecha_inicio.strftime("%Y-%m-%d %H:%M:%S"),
        'fecha_fin': fecha_fin.strftime("%Y-%m-%d %H:%M:%S")
    })


def getSensorData(sensor):
    if stations_by_minute and str(sensor['e_cod']) in stations_by_minute:
        sendSensorDataV2(copy.deepcopy(sensor), __getSensorDataByMinute(sensor))
    else:
        sendSensorData(copy.deepcopy(sensor), __getSensorDataByHour(sensor))


def getAllSensorDatas(station_sensors):
    for sensor in station_sensors:
        try:
            getSensorData(sensor)
            # processThread = threading.Thread(target=getSensorData, args=(sensor,))
            # processThread.start()
        except Exception as e:
            print(e)
            pass


def __convertDate(date_time_str, timezone_from=CEAZAMET_TIMEZONE, timezone_to="UTC"):
    date_time_obj = datetime.datetime.strptime(date_time_str, '%Y-%m-%d %H:%M:%S')
    timezone_date_time_obj = timezone(timezone_from).localize(date_time_obj)
    return timezone_date_time_obj.astimezone(timezone(timezone_to))


def __getStationsByMinute():
    stations = ["8", "6", "PC", "MARPCH"]
    # try:
    #     url = CEAZAMET_URL + CEAZAMET_NETWORK_STATE_WS
    #     res = requests.get(url)
    #
    #     parser = etree.HTMLParser()
    #     tree = etree.fromstring(res.content.replace('</tr>', '</tr><tr>'), parser)
    #     results = tree.xpath('//tr/td[position()=3]')
    #
    #     for r in results:
    #         if r.text.startswith('cmet_'):
    #             print(r.text.replace('cmet_', ''))
    #             # stations.append(r.text.replace('cmet_', ''))
    # except Exception as e:
    #     print(e)
    return stations


def main():
    global stations_by_minute

    stations_by_minute = __getStationsByMinute()

    print("Total Stations by Minute %s" % (len(stations_by_minute)))

    station_sensors = loadStationSensors(FORCE_RELOAD)

    print("Total Sensors %s" % (len(station_sensors)))

    while(True):
        # getAllSensorDatas(station_sensors)
        processThread = threading.Thread(target=getAllSensorDatas, args=(station_sensors,))
        processThread.start()
        time.sleep(SLEEP_TIME)


def init():
    global CEAZAMET_USER
    global FORCE_RELOAD
    global SLEEP_TIME
    global influxdb_client

    parser = argparse.ArgumentParser()
    parser.add_argument("--user", help="Caeza-Met request user [default: %s]" % (CEAZAMET_USER), type=str, default=CEAZAMET_USER)
    parser.add_argument("--reload", help="Reload stations info? [default: %s]" % (FORCE_RELOAD), type=bool, default=FORCE_RELOAD)
    parser.add_argument("--time", help="Time (in seconds) to request new data [default: %s]" % (SLEEP_TIME), type=int, default=SLEEP_TIME)
    args = parser.parse_args()

    CEAZAMET_USER = args.user
    SLEEP_TIME = args.time
    FORCE_RELOAD = args.reload

    influxdb_client = InfluxDBClient(INFLUXDB_HOST, INFLUXDB_PORT, INFLUXDB_USER, INFLUXDB_PASSWORD, INFLUXDB_DBNAME)
    try:
        influxdb_client.create_database(INFLUXDB_DBNAME)
    except InfluxDBClientError as e:
        print(e)
        pass


if __name__ == "__main__":
    init()
    main()
