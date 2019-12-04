"""
Author : Pierre Chemla & Elie Ganancia

This script is main script of our scrapping project.
It launches the scrapping of all artists/labels/clubs/events.

If the user wants to scrap information of only a sample of artists/labels/events, Please uncomment the line in the main
function.
Otherwise the code will scrap all labels (+15 000 url page) and all events (~6000).

"""

import poc_scrapping_ra_pierre as sp
import poc_web_scrapp as se
import SQL_Web_Scrapping_RA as ra_sql
import pandas as pd
import argparse
import os

DB_FILENAME = "Resident_Advisor_db"


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
                                       "\n Usage example : (1) main_scrapping.py "
                                       "-scrap_labels -scrap_artists \n (2) main_scrapping.py \n"
                                       "(3) main_scrapping.py -scrap_labels -get_csv /// ")

parser.add_argument('-scrap_labels', action="store_true", default=False)
parser.add_argument('-scrap_artists', action="store_true", default=False)
parser.add_argument('-scrap_events', action="store_true", default=False)
parser.add_argument('-scrap_clubs', action="store_true", default=False)

pd.set_option('display.max_columns', 500)


def launch_scrapping(labels_, artists_, events_, clubs_):
    if labels_:
        url_labels = "https://www.residentadvisor.net/labels.aspx?show=all"
        data_labels = sp.get_labels(url_labels)
        print("\n")
        print("Scrappy Coco just found a listing of all labels in Resident Advisor "
              "({0} labels)".format(data_labels.shape[0]))
        print("Please find below a sample of labels found : \n")
        print(data_labels.head(10))
        print("\n")
        print("Thanks to Scrappy Coco, we have a listing of labels. \n But he can do "
              "better !!! \n Scrappy Coco will now get some details "
              "for each of these labels")
        print("\n")
        data_labels_information = sp.get_label_information(data_labels)
        ra_sql.insert_label(data_labels_information, DB_FILENAME)
    else:
        data_labels = None
        data_labels_information = None

    if artists_:
        url_artists = "https://www.residentadvisor.net/dj.aspx"
        data_artists = sp.get_artists(url_artists)
        print("\n")
        print("Scrappy Coco just found a listing of all artists in Resident Advisor "
              "({0} labels)".format(data_artists.shape[0]))
        print("Please find below a sample of artists found : \n")
        print(data_artists.head(10))
        print("\n")
        print("Thanks to Scrappy Coco, we have a listing of artists. \n But he can do "
              "better !!! \n Scrappy Coco will now get some details "
              "for each of these artist")
        print("\n")
        data_artists_information = sp.get_artist_information(data_artists)
        ra_sql.insert_artist(data_artists_information, DB_FILENAME)
    else:
        data_artists = None
        data_artists_information = None

    if events_:
        data_countries = se.get_countries()
        print("As required, Scrappy Coco will now get some details "
              "for each events (parties) on Resident Advisor")
        print("!!!! You may have time to buy a coffee, please do not forget to bring one for "
              "Scrappy Coco (Americano) :) !!!!!!")
        print("\n")
        data_events = se.get_events(data_countries,DB_FILENAME)
    else:
        data_events = None

    if clubs_:
        data_countries_id = se.get_countries_id()
        print("As required, Scrappy Coco will now get some details "
              "for each events (clubs) on Resident Advisor")
        print("!!!! You may have time to buy a coffee, please do not forget to bring one for "
              "Scrappy Coco (Americano) :) !!!!!!")
        print("\n")
        data_clubs = se.get_clubs(data_countries_id,DB_FILENAME)
    else:
        data_clubs = None


def main():
    args = parser.parse_args()
    label_arg = False
    club_arg = False
    event_arg = False
    artist_arg = False
    print("\n")
    print("!!! Hello !!!!")
    print("\n")
    print("My name is Scrappy Coco, I can scrap many things on Resident Advisor. I can "
          "also do many other things but it is not relevant for now :) ")
    print("(Sometimes I speak of me in the third person, please don't judge me !!)")
    print("\n")


    if (not args.scrap_labels) and (not args.scrap_events) and (not args.scrap_clubs) and (not args.scrap_artists):
        print("You did not specified which information to scrapp (artists/club/event/label).")
        print("Scrappy Coco will scrapp everything on Resident Advisor")
        print("\n")
        print("!!!! You may have time to buy a coffee, please do not forget to bring one for "
              "Scrappy Coco (Americano) :) !!!!!!")
        print("\n")
        launch_scrapping(True, True, True, True)
    else:
        if args.scrap_labels:
            print("Your required the scrapping of labels")
            print("\n")
            label_arg = True
        if args.scrap_clubs:
            print("You required the scrapping of clubs")
            print("\n")
            club_arg = True
        if args.scrap_events:
            print("You required the scrapping of events")
            print("\n")
            event_arg = True
        if args.scrap_artists:
            print("You required the scrapping of artists")
            print("\n")
            artist_arg = True
        launch_scrapping(label_arg, artist_arg, event_arg, club_arg)


if __name__ == '__main__':
    main()
