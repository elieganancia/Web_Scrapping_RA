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
import pandas as pd


pd.set_option('display.max_columns', 500)


def main():
    url_labels = "https://www.residentadvisor.net/labels.aspx?show=all"
    data_labels = sp.get_labels(url_labels)

    #data_labels = data_labels.iloc[:200, :]

    data_labels_information = sp.get_label_information(data_labels)


    url_artists = "https://www.residentadvisor.net/dj.aspx"
    data_artists = sp.get_artists(url_artists)

    #data_artists = data_artists.iloc[:200, :]

    data_artists_information = sp.get_artist_information(data_artists)


    data_countries = se.get_countries()
    #data_countries = data_countries.iloc[200, :]

    data_events = se.get_events(data_countries)
    data_clubs = se.get_clubs()


if __name__ == '__main__':
    main()