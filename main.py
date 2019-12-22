"""
Author : Pierre Chemla & Elie Ganancia

This script is main script of our scrapping project.
Based on CLI argumentsof the user it will launch the scrapping of the Web site residentadvisor.net


"""

import Artist_Label_Web_Scrapping_RA as sp
import Event_Club_Web_Scrapping_RA as se
import SQL_Web_Scrapping_RA as ra_sql
import API_Meteo_Web_Scrapping_RA as api_meteo
from Logger_Web_Scrapping_RA import Scrappy_logger
from Logger_Web_Scrapping_RA import Scrappy_info
import pandas as pd
import argparse

DB_FILENAME = "Data_Resident_Advisor_Sunday_lt4"


parser = argparse.ArgumentParser(usage="main_scrapping.py [-scrap_labels] [-scrap_artists] [-scrap_events] "
                                       "[-scrap_clubs] [-get_csv]"
                                       " \n /// This program will ask to Scrappy Coco to get some information"
                                       " from the website Resident Advisor. \n If the user do not specify options,"
                                       " it will scrap everything and store it in a database. \n"
                                       "Option Description : \n"
                                       "-scrap_labels : specify this option if you want to scrap labels \n"
                                       "-scrap_artists : specify this option if you want to scrap artists \n"
                                       "-scrap_events : specify this option if you want to scrap events \n"
                                       "-scrap_clubs : specify this option if you want to scrap clubs \n"
                                       "-erase_database : specify this option if you want to delete your \n"
                                       "-get_external_data : specify this option if you want to add external "
                                       "data such as spotify data and meteo data \n"
                                       "resident advisor database \n"
                                       "\n Usage example : (1) main_scrapping.py "
                                       "-scrap_labels -scrap_artists \n (2) main_scrapping.py \n"
                                       "(3) main_scrapping.py -scrap_labels -get_csv /// ")

parser.add_argument('-scrap_labels', action="store_true", default=False)
parser.add_argument('-scrap_artists', action="store_true", default=False)
parser.add_argument('-scrap_events', action="store_true", default=False)
parser.add_argument('-scrap_clubs', action="store_true", default=False)
parser.add_argument('-erase_database', action="store_true", default=False)
parser.add_argument('-get_external_data', action="store_true", default=False)

pd.set_option('display.max_columns', 500)

scrappy_log = Scrappy_logger()
scrappy_info = Scrappy_info()


def launch_scrapping(labels_, artists_, events_, clubs_, erase_, external_api_):

    if erase_:
        ra_sql.erase_database(DB_FILENAME)

    ra_sql.database_check(DB_FILENAME)

    if labels_:
        url_labels = "https://www.residentadvisor.net/labels.aspx?show=all"
        data_labels = sp.get_labels(url_labels, scrappy_info, scrappy_log)
        scrappy_info.info("\n")
        scrappy_info.info("Scrappy Coco just found a listing of all labels in Resident Advisor "
              "({0} labels)".format(data_labels.shape[0]))
        scrappy_info.info("Please find below a sample of labels found : \n")
        scrappy_info.info(data_labels.head(10))
        scrappy_info.info("\n")
        scrappy_info.info("Thanks to Scrappy Coco, we have a listing of labels. \n But he can do "
              "better !!! \n Scrappy Coco will now get some details "
              "for each of these labels \n")
        data_labels_information = sp.get_label_information(data_labels, DB_FILENAME, scrappy_info, scrappy_log)
    else:
        data_labels = None
        data_labels_information = None

    if artists_:
        url_artists = "https://www.residentadvisor.net/dj.aspx"
        data_artists = sp.get_artists(url_artists, scrappy_info, scrappy_log)
        scrappy_info.info("\n")
        scrappy_info.info("Scrappy Coco just found a listing of all artists in Resident Advisor "
              "({0} labels)".format(data_artists.shape[0]))
        scrappy_info.info("Please find below a sample of artists found : \n")
        scrappy_info.info(data_artists.head(10))
        scrappy_info.info("\n Thanks to Scrappy Coco, we have a listing of artists. \n But he can do "
              "better !!! \n Scrappy Coco will now get some details "
              "for each of these artist \n")
        data_artists_information = sp.get_artist_information(data_artists, DB_FILENAME, scrappy_info, scrappy_log)

        ra_sql.insert_artist(data_artists, DB_FILENAME)
        if external_api_:
            ra_sql.update_artist_info(DB_FILENAME)
            ra_sql.insert_artist_track(DB_FILENAME)
    else:
        data_artists = None
        data_artists_information = None

    if events_:
        data_countries = se.get_countries()
        scrappy_info.info("As required, Scrappy Coco will now get some details "
              "for each events (parties) on Resident Advisor")
        scrappy_info.info("!!!! You may have time to buy a coffee, please do not forget to bring one for "
              "Scrappy Coco (Americano) :) !!!!!! \n")
        data_events = se.get_events(data_countries,DB_FILENAME, scrappy_info, scrappy_log)
        if external_api_:
            data_meteo = api_meteo.get_meteo_information(DB_FILENAME)
            ra_sql.insert_meteo(data_meteo, DB_FILENAME)
    else:
        data_events = None

    if clubs_:
        data_countries_id = se.get_countries_id(scrappy_info, scrappy_log)
        scrappy_info.info("As required, Scrappy Coco will now get some details "
              "for each clubs on Resident Advisor")
        scrappy_info.info("!!!! You may have time to buy a coffee, please do not forget to bring one for "
              "Scrappy Coco (Americano) :) !!!!!! \n")
        data_clubs = se.get_clubs(data_countries_id,DB_FILENAME, scrappy_info, scrappy_log)
    else:
        data_clubs = None


def main():
    args = parser.parse_args()
    label_arg = False
    club_arg = False
    event_arg = False
    artist_arg = False
    erase_arg = False
    external_info = False

    scrappy_info.info("\n !!! Hello !!!! \n")
    scrappy_info.info("My name is Scrappy Coco, I can scrap many things on Resident Advisor. I can "
          "also do many other things but it is not relevant for now :) ")
    scrappy_info.info("(Sometimes I speak of me in the third person, please don't judge me !!) \n")

    scrappy_log.info("SCRAPPY IS READY TO WORK !!! LET'S SCRAPP")

    if (not args.scrap_labels) and (not args.scrap_events) and (not args.scrap_clubs) and (not args.scrap_artists):
        scrappy_info.info("You did not specified which information to scrapp (artists/club/event/label).")
        scrappy_info.info("Scrappy Coco will scrapp everything on Resident Advisor \n")
        scrappy_info.info("!!!! You may have time to buy a coffee, please do not forget to bring one for "
              "Scrappy Coco (Americano) :) !!!!!! \n")
        launch_scrapping(True, True, True, True, True,True)
    else:
        if args.scrap_labels:
            scrappy_info.info("Your required the scrapping of labels \n")
            label_arg = True
        if args.scrap_clubs:
            scrappy_info.info("You required the scrapping of clubs \n")
            club_arg = True
        if args.scrap_events:
            scrappy_info.info("You required the scrapping of events \n")
            event_arg = True
        if args.scrap_artists:
            scrappy_info.info("You required the scrapping of artists \n")
            artist_arg = True
        if args.erase_database:
            scrappy_info.info("You required the deletion of your resident advisor \n")
            erase_arg = True
        if args.get_external_data:
            scrappy_info.info("Your required some data from external sources/api \n")
            external_info = True
        launch_scrapping(label_arg, artist_arg, event_arg, club_arg, erase_arg, external_info)


if __name__ == '__main__':
    main()
