import mysql.connector
import numpy as np
import mysql.connector
from googlesearch import search
from bs4 import BeautifulSoup
import requests
from unidecode import unidecode
import pandas as pd
import re


def get_facebook_data(DB_FILENAME):
    """
    This function try for each event to scrap data on the facebook event page (interested/going people). It will first search on google the url of the event and then scrapp the data
    on the event facebook page. These data are only available on the meta part of the html pages.
    @param DB_FILENAME: database file name
    @type DB_FILENAME: string
    @return: a pandas dataframe with the collected data
    @rtype: pandas dataframe
    """

    mydb = mysql.connector.connect(host="localhost", user="resident_advisor", passwd="bicep",
                                       auth_plugin='mysql_native_password')
    cur = mydb.cursor(buffered=True,dictionary=True)
    cur.execute("USE " + DB_FILENAME)

    query = "SELECT id FROM events_information;"
    cur.execute(query)
    ids_events = cur.fetchall()

    #event_name
    int_values = []
    going_values = []

    for el in ids_events:
        print("--- Calling the api for the event : #{} ".format(el['id']))
        id_ = el['id']

        query = "SELECT event_name, event_date FROM events_information WHERE id = " + "'" + str(id_) + "'"
        cur.execute(query)
        result_ = cur.fetchall()[0]
        name_ = result_['event_name']
        date_ = result_['event_date']

        # to search
        query_fb = name_ + " facebook"
        test = search(query_fb, stop=3)

        url_pages = []
        for el in test:
            url_ = el
            url_pages.append(url_)

        print(date_)


        url_type_fb = "www.facebook.com/events/"
        ind_url_cand = [i for i,el in enumerate(url_pages) if url_type_fb in el]
        if (len(ind_url_cand) > 0) and ('20' in date_ ):
            event_page_ = url_pages[ind_url_cand[0]]

            try:
                with requests.Session() as res:
                    page_ = res.get(event_page_)

                soup_return = BeautifulSoup(page_.content, 'html.parser')
                meta_ = soup_return.find_all('meta')[3].attrs['content']
            except:
                meta_ = None

            day_ = date_.split(" ")[0]
            year_ = date_.split(" ")[2]
            search_date = day_ + " " + year_
            content_flag = False
            if meta_ is not None:
                if search_date in unidecode(meta_):
                    content_flag = True

            if content_flag:

                content_audience_int = re.search(r'(?<=with )(.*)(?= people interested)', meta_)
                content_audience_going = re.search(r'(?<=interested and )(.*)(?= people going)', meta_)
                try:
                    if content_audience_int is not None:
                        int_value_ = int(content_audience_int.group(0))
                        print("{} people interested".format(int_value_))
                    else:
                        int_value_ = None

                    if content_audience_going is not None:
                        going_value_ = int(content_audience_going.group(0))
                        print("{} people going".format(going_value_))
                    else:
                        going_value_ = None
                except:
                    int_value_ = None
                    going_value_ = None

                if (int_value_ is None) and (going_value_ is None):
                    print("No Facebook Data for this event")

            else:
                print("No Facebook Data for this event")
                int_value_ = None
                going_value_ = None

        else:
            print("No Facebook data for this event")
            int_value_ = None
            going_value_ = None

        int_values.append(int_value_)
        going_values.append(going_value_)
    ids_values = [el['id'] for el in ids_events]
    return pd.DataFrame({'id':ids_values,'interested':int_values,'going':going_values})


def update_events_fb(data_, db_filename):
    """
    This function receives data from the get_facebook_data() function znd update our database (events_information table)
    @param db_filename: database filename
    @type db_filename: string
    @return: nothing
    @rtype: na
    """

    mydb = mysql.connector.connect(host="localhost", user="resident_advisor", db=db_filename, passwd="bicep",
                                   auth_plugin='mysql_native_password')
    cur = mydb.cursor()

    for i in range(data_.shape[0]):
        cur.execute(''' UPDATE events_information SET fb_interested=%s, fb_going=%s where id=%s''',[np.int(data_['interested'][i]), np.int(data_['going'][i]),np.int(data_["id"][i])])
    mydb.commit()
    cur.close()


