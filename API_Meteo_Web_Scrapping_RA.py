"""
Author : Elie Ganancia and Pierre Chemla

This python script contains the get_meteo_information() function used to call the meteostat api and get meteo data.
"""

import mysql.connector
import Artist_Label_Web_Scrapping_RA as sp
import json
import datetime
import pandas as pd

METEO_KEY = "OHdum6zw"


def get_meteo_information(DB_FILENAME):
    """
    The get_meteo_information() function get external data using the api meteostat.
    This function first select date and location of each event present in our database.
    It then requests the meteostat, calling a specific url, get more meteo data such as
    temperature/humidity/snow/precipitations.
    @param DB_FILENAME: Database name
    @type DB_FILENAME: string
    @return: a dataframe containing api external data
    @rtype: pandas dataframe
    """

    mydb = mysql.connector.connect(host="localhost", user="resident_advisor", passwd="bicep",
                                   auth_plugin='mysql_native_password')
    cur = mydb.cursor(buffered=True,dictionary=True)
    cur.execute("USE " + DB_FILENAME)

    query = "SELECT id FROM events_information;"
    cur.execute(query)
    ids_events = cur.fetchall()

    meteo_ids = []
    meteo_temperatures = []
    meteo_humidities = []
    meteo_precipitations = []
    meteo_snows = []

    for el in ids_events:
        print("--- Calling the api for the event : #{} ".format(el['id']))
        id_ = el['id']
        try:
            query = "SELECT event_date, country_id_ra FROM events_information WHERE id = " + "'" + str(id_) + "'"
            cur.execute(query)
            result_ = cur.fetchall()[0]
            date_ = result_['event_date']


            try:
                if result_['country_id_ra'].split(",")[0] == 'All':
                    location_ = result_['country_id_ra'].split(",")[1].strip()
                else:
                    location_ = result_['country_id_ra'].split(",")[0].strip()

                date_ = datetime.datetime.strptime(date_, '%d %b %Y')
                date_location_flag = True
            except:
                date_location_flag = False

            meteo_flag = False

            if date_location_flag:

                #### Get station
                query_meteo = "https://api.meteostat.net/v1/stations/search?q=" + location_ +"&key=" + METEO_KEY

                if len(str(sp.get_content(query_meteo))) > 0:
                    station_content = json.loads(str(sp.get_content(query_meteo)))

                    if len(station_content["data"]) == 0:
                        station_id = False
                    else:
                        station_id = station_content["data"][0]['id']
                else:
                    station_id = False

                if station_id != False:
                    date_meteo = str(date_.year) + "-" + str(date_.month) + "-" + str(date_.day)
                    query_meteo = "https://api.meteostat.net/v1/history/hourly?station="+ station_id +"&start=" + date_meteo + \
                                  "&end=" + date_meteo +"&time_format=Y-m-d%20H:i&key=" + METEO_KEY

                    meteo_content = json.loads(str(sp.get_content(query_meteo)))
                    meteo_hour = False
                    try:
                        if len(meteo_content['data'])>0:
                            for meteo_el in meteo_content['data']:
                                time_ = int(meteo_el['time'].split(" ")[1].split(":")[0])
                                if time_ == 20:
                                    meteo_hour = True
                                    temperature_ = meteo_el['temperature']
                                    humidity_ = meteo_el['humidity']
                                    precipitation_ = meteo_el['precipitation']
                                    snow_ = meteo_el['snowdepth']
                            if not meteo_hour:
                                base_element_ = meteo_content['data'][0]
                                temperature_ = base_element_['temperature']
                                humidity_ = base_element_['humidity']
                                precipitation_ = base_element_['precipitation']
                                snow_ = base_element_['snowdepth']
                        meteo_flag = True
                    except:
                        meteo_flag = False

            if meteo_flag:
                meteo_ids.append(id_)
                meteo_temperatures.append(temperature_)
                meteo_humidities.append(humidity_)
                meteo_precipitations.append(precipitation_)
                meteo_snows.append(snow_)
            else:
                print("No meteo found for this event")
        except:
            print("No meteo found for this event")


    return pd.DataFrame({'event_id':meteo_ids,'temperature':meteo_temperatures,'humidity':meteo_humidities,
                         'precipitation':meteo_precipitations,'snow':meteo_snows})