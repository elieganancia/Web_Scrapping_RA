import mysql.connector
from unidecode import unidecode

import poc_scrapping_ra_pierre as sp
import poc_web_scrapp as se
import SQL_Web_Scrapping_RA as ra_sql

import requests
import json
import datetime
import pandas as pd

DB_FILENAME = "Data_Resident_Advisor"

mydb = mysql.connector.connect(host="localhost", user="resident_advisor", passwd="bicep",
                               auth_plugin='mysql_native_password')
cur = mydb.cursor()

def database_check(DB_FILENAME):
    mydb = mysql.connector.connect(host="localhost", user="resident_advisor", passwd="bicep",
                                   auth_plugin='mysql_native_password')
    cur = mydb.cursor()

    query = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = " + "'" + DB_FILENAME + "'"

    cur.execute(query)
    result = cur.fetchall()
    if len(result) == 0:
        print("The database does not exist ..... No worries")
        print("Scrappy Coco will creat one for you")
        ra_sql.create_table_ra(DB_FILENAME)
    else:
        print("The database already exists. Scrappy Coco will update it.")

database_check(DB_FILENAME)


###############################################


data_countries = se.get_countries()
data_countries = data_countries[:5]
data_events = se.get_events(data_countries,DB_FILENAME)



pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)




def get_meteo_information():
    print("Getting some meteo information")

date_ = datetime.datetime.strptime(test['Event_Date'],'%d %b %Y')
country_ = test['Country_ID'].split(",")[1].strip()



mydb = mysql.connector.connect(host="localhost", user="resident_advisor", passwd="bicep",
                               auth_plugin='mysql_native_password')
cur = mydb.cursor(buffered=True,dictionary=True)
cur.execute("USE " + DB_FILENAME)

cur.execute("SHOW TABLES")
tables = cur.fetchall()


################################################################################################
query_meteo = "https://api.meteostat.net/v1/stations/search?q=paris&key=OHdum6zw"
###############################################################################################


query = "SELECT id FROM events_information;"
cur.execute(query)
ids_events = cur.fetchall()

# for id in ids_events id_ = ids_events[0i]['id']

METEO_KEY = "OHdum6zw"

id_ = 0

query = "SELECT event_date, country_id_ra FROM events_information WHERE id = " + "'" + str(id_) + "'"
cur.execute(query)
result_ = cur.fetchall()[0]
date_ = result_['event_date']
if result_['country_id_ra'].split(",")[0] == 'All':
    location_ = result_['country_id_ra'].split(",")[1].strip()
else:
    location_ = result_['country_id_ra'][0]

date_ = datetime.datetime.strptime(date_, '%d %b %Y')



#### Get station
query_meteo = "https://api.meteostat.net/v1/stations/search?q=" + location_ +"&key=" + METEO_KEY
station_content = json.loads(str(sp.get_content(query_meteo)))

if len(station_content["data"]) == 0:
    print("No meteo found for this city/country")
    station_id = False
else:
    station_id = station_content["data"][0]['id']

if station_id != False:
    date_meteo = str(date_.year) + "-" + str(date_.month) + "-" + str(date_.day)
    query_meteo = "https://api.meteostat.net/v1/history/hourly?station="+ station_id +"&start=" + date_meteo + \
                  "&end=" + date_meteo +"&time_format=Y-m-d%20H:i&key=" + METEO_KEY

    meteo_content = json.loads(str(sp.get_content(query_meteo)))
    meteo_flag = False

    if len(meteo_content['data']>0):
        for el in meteo_content['data']:
            time_ = int(el['time'].split(" ").split(":")[0])
            if time_ == 20:
                meteo_flag = True
                temperature_ = el['temperature']
                humidity_ = el['humidity']
                precipitation = el['precipitation']
                snow_ = el['snowdepth']
        if not meteo_flag:
            meteo_flag = True
            base_element_ = meteo_content['data'][0]
            temperature_ = base_element_['temperature']
            humidity_ = base_element_['humidity']
            precipitation = base_element_['precipitation']
            snow_ = base_element_['snowdepth']
else:
    meteo_flag= True
    temperature_ = None
    humidity_ = None
    precipitation_ = None
    snow_ = None

# table id_event / temperature / humidity / precipitation / neige




















###################################################################################################################
###################################################################################################################
###################################### FB SCrap / Google search ###################################################
###################################################################################################################
###################################################################################################################
###################################################################################################################

import wikipedia
print wikipedia.summary("Wikipedia")
# Wikipedia (/ˌwɪkɨˈpiːdiə/ or /ˌwɪkiˈpiːdiə/ WIK-i-PEE-dee-ə) is a collaboratively edited, multilingual, free Internet encyclopedia supported by the non-profit Wikimedia Foundation...

wikipedia.search("Barack")

test = wikipedia.page("Ubuntu").html()

import google
from googlesearch import search
# to search
query = "dan shake italojohnson budx facebook"
test = search(query, num=10)


for j in google.search(query, tld="co.in", num=10, stop=1, pause=2):
    print(j)

for j in test:
    print(j)


from bs4 import BeautifulSoup
import requests
from urllib import parse
import pandas as pd
import time

url_ = "https://www.facebook.com/events/art-club/budxtlv-red-axes-friends-711-art-club/760073021105127/"

with requests.Session() as res:
    page_ = res.get(url_)

soup_return = BeautifulSoup(page_.content, 'html.parser')

test = soup_return.find_all('meta')
test[3].attrs['content']



url_ = "https://www.facebook.com/events/alphabet/budx-complex-dan-shake-italojohnson-dj-deep-kapitan-1400/489443115004611/"

with requests.Session() as res:
    page_ = res.get(url_)

soup_return = BeautifulSoup(page_.content, 'html.parser')



