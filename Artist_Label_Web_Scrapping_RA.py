"""
Author: Elie Ganancia & Pierre Chemla

This script conatains functions used on the main script of the scrapping project (main_scrapping.py)
These functions are used to get all information of Artists and Labels on Resident Advisor
"""
##################
###  Library  ####
##################

from bs4 import BeautifulSoup
import requests
from urllib import parse
import pandas as pd
import time


###################
###  Utilities  ###
###################


def get_content(url_):
    """
    This function get the content of the online page url_
    :param url_: url of the online page
    :return: the html content of this online page
    """
    time.sleep(0.1)
    with requests.Session() as res:
        page_ = res.get(url_)

    soup_return = BeautifulSoup(page_.content, 'html.parser')
    return soup_return


################################################################################################################
###############################     Scrapping     ##############################################################
################################################################################################################


#######################
### Label Database ####
#######################


##### Get list of labels

def get_labels(url_labels_):
    """
    This function get basic information (name/url/id) of all the labels on Resident advisor
    The urls will be used to get information for each label
    :param url_labels_: url of the page where to get these basic information
    :return: a pandas dataframe which contains these basic information
    """
    print("//////////////////////////////////////////////////////////////////////////////////////////")
    print("Getting all labels of Resident Advisor")
    print("//////////////////////////////////////////////////////////////////////////////////////////")
    print("\n")

    BASE_URL = "https://www.residentadvisor.net"
    label_list_content = get_content(url_labels_)
    list_letters = label_list_content.findAll(class_='fl pr8')

    label_names = []
    label_urls = []
    label_ids = []
    #we loop first over all letters
    for i in range(len(list_letters)):
        # we get liste of label for each letters
        list_label_letter = list_letters[i].findAll('a')
        for j in range(len(list_label_letter)):
            label_ = list_label_letter[j].get_text()
            if label_ != "":
                url_ = list_label_letter[j].get("href")
                id_ = parse.parse_qsl(parse.urlsplit(url_).query)[0][1]
                label_names.append(label_)
                label_urls.append(url_)
                label_ids.append(id_)


    data_labels_return = pd.DataFrame({'name':label_names, 'url':label_urls, 'id':label_ids})
    print("{0} labels have been found on Resident Advisor".format(data_labels_return.shape[0]))
    print("\n")
    data_labels_return['url'] = BASE_URL + data_labels_return['url']

    return data_labels_return


######---> specific labels


def get_label_information(data_labels_):
    """
    This function get information on each labels url page
    :param data_labels_: dataframe cotaining url pages of labels
    :return: a dataframe containing specifics information for each artists (name, date, country, online account,
     followers and description)
    """
    list_url_label = list(data_labels_['url'])
    list_name_label = list(data_labels_['name'])
    list_ids_label = list(data_labels_['id'])

    print("//////////////////////////////////////////////////////////////////////////////////////////")
    print("       The script is getting information for all labels ({0} labels)".format(len(list_url_label)))
    print("//////////////////////////////////////////////////////////////////////////////////////////")
    print("\n")

    date_creation_labels = []
    location_labels = []
    online_urls = []
    label_popularities = []
    label_description = []
    label_artists = []

    for url_ in list_url_label:
        print("Getting information on : {0}".format(url_))
        label_information_content = get_content(url_)

        ### first type of information
        first_content = label_information_content.findAll(class_="fl col4-6 small")[0].findAll(class_="clearfix")[1]
        content_list = first_content.findAll('li')

        date_ = False
        location_label_ = False
        online_ = False

        for el in content_list:
            if "established" in el.find("div").get_text().lower():
                date_creation_labels.append(el.get_text()[-4:])
                date_ = True
            if "location" in el.find("div").get_text().lower():
                location_labels.append(el.findAll('a')[0].get_text().strip())
                location_label_ = True
            if "internet" in el.find("div").get_text().lower():
                label_urls = []
                list_of_urls = el.findAll('a')
                for url_el in list_of_urls:
                    label_urls.append(url_el.get('href'))
                online_urls.append(label_urls)
                online_ = True

        if not date_:
            date_creation_labels.append(None)
        if not location_label_:
            location_labels.append(None)
        if not online_:
            online_urls.append(None)

        ### second type of information (number of follower)
        second_content = label_information_content.findAll(class_="fav button clearfix")
        label_popularities.append(
            second_content[0].findAll(id="MembersFavouriteCount")[0].get_text().strip().replace(",", ""))

        ### third type of information (description)
        third_content = label_information_content.findAll(class_="record-label-blurb")
        label_description.append(third_content[0].get_text())

        ### fourth type of information (description)
        try:
            fourth_content = label_information_content.findAll(class_='grid standard')[0].findAll("a")
            list_artists_label = []
            for el in fourth_content:
                list_artists_label.append(el.get("href").split("/")[-1])

        except:
            list_artists_label = None
        label_artists.append(list_artists_label)

    data_label_information_return = pd.DataFrame({'Name': list_name_label, 'Creation': date_creation_labels,
                                                  'Country': location_labels, 'Online_account': online_urls,
                                                  'Followers': label_popularities, 'Description': label_description,
                                                  'id': list_ids_label,"ids_artists": label_artists})
    print("\n")

    return data_label_information_return



########################
### Artist Database ####
########################


def get_artists(url_artists_):
    """
    This function get basic information (name/url/id) of all the artists on Resident advisor
    The urls will be used to get information for each label
    :param url_artists_: url of the page where to get these basic information
    :return: a pandas dataframe which contains these basic information
    """
    print("//////////////////////////////////////////////////////////////////////////////////////////")
    print("Getting all artists of Resident Advisor")
    print("//////////////////////////////////////////////////////////////////////////////////////////")
    print("\n")

    BASE_URL = "https://www.residentadvisor.net"
    artist_list_content = get_content(url_artists_)
    list_letters = artist_list_content.findAll(class_='fl pr8')

    artist_names = []
    artist_urls = []
    artist_ids = []
    # we loop first over all letters
    for i in range(len(list_letters)):
        # we get a list of label for each letters
        list_artist_letter = list_letters[i].findAll('a')
        for j in range(len(list_artist_letter)):
            artist_ = list_artist_letter[j].get_text()
            if artist_ != "":
                url_ = list_artist_letter[j].get("href")
                id_ = url_.split("/")[-1]
                artist_names.append(artist_)
                artist_urls.append(url_)
                artist_ids.append(id_)


    data_artist_return = pd.DataFrame({'name':artist_names, 'url':artist_urls, 'id':artist_ids})
    data_artist_return['url'] = BASE_URL + data_artist_return['url']
    print("{0} artists have been found on Resident Advisor".format(data_artist_return.shape[0]))

    return data_artist_return


# specific artist

def get_artist_information(data_artist_):
    """
    This function get information on each artist url page
    :param data_artist_: dataframe cotaining url pages of artists
    :return: a dataframe containing specifics information for each artists (name, country, online account, alliases,
     followers, description, collaboration, famous location, famous club , url & ids)
    """
    list_url_artist = list(data_artist_['url'])
    list_ids = list(data_artist_['id'])
    list_artist_dj_names = list(data_artist_['name'])
    print("//////////////////////////////////////////////////////////////////////////////////////////")
    print("The script is getting information for all artists ({0} artists)".format(len(list_url_artist)))
    print("//////////////////////////////////////////////////////////////////////////////////////////")
    print("\n")

    artist_names = []
    artist_basis_locations = []
    online_urls = []

    artist_popularities = []
    artist_descriptions = []
    artist_collaborations = []
    artist_famous_locations = []
    artist_famous_clubs = []
    artist_aka = []

    for url_ in list_url_artist:
        artist_information_content = get_content(url_)
        print("Getting information on : {0}".format(url_))
        # First type of Information (names/location/online account)
        first_content = artist_information_content.findAll(class_="col4-6 small fl")[0].findAll(class_="clearfix")[1]
        content_list = first_content.findAll('li')

        country_ = False
        online_ = False
        name_ = False
        aliases_ = False
        for el in content_list:
            if "country" in el.find("div").get_text().lower():
                artist_basis_locations.append(el.findAll("a")[0].get_text().strip())
                country_ = True
            if "internet" in el.find("div").get_text().lower():
                artist_urls = []
                list_of_urls = el.findAll('a')
                for url_el in list_of_urls:
                    artist_urls.append(url_el.get('href'))
                online_urls.append(artist_urls)
                online_ = True
            if "name" in el.find("div").get_text().lower():
                artist_names.append(el.get_text().split("/")[-1])
                name_ = True
            if "aliases" in el.find("div").get_text().lower():
                artist_aka.append(el.get_text().split("/")[-1])
                aliases_ = True

        if not country_:
            artist_basis_locations.append(None)
        if not online_:
            online_urls.append(None)
        if not name_:
            artist_names.append(None)
        if not aliases_:
            artist_aka.append(None)


        # Second type of information (popularity)

        second_content = artist_information_content.findAll(class_="fav button clearfix")[0]
        artist_popularities.append(second_content.findAll(id="MembersFavouriteCount")[0].get_text().strip().replace(",",""))

        # Third type of information (description)
        try:
            third_content = artist_information_content.findAll(class_="excerpt mobile-pr24-tablet-desktop-pr8 pt8")[0]
            artist_descriptions.append(third_content.get_text())
        except:
            artist_descriptions.append(None)

        # Fourth type of information :
        if len(artist_information_content.findAll(class_="countstats stats"))>0:
            fourth_content_temp = artist_information_content.findAll(class_="countstats stats")[0]
            fourth_content = fourth_content_temp.findAll(class_="stats-list list clearfix")[0].findAll("li")

            ### Appears most with
            try:
                most_played_artist_with = []
                most_played_artist_with_content = fourth_content[0].findAll("a")
                for el in most_played_artist_with_content:
                    most_played_artist_with.append(el.get("href").split("/")[-1])
                artist_collaborations.append(most_played_artist_with)
            except:
                artist_collaborations.append(None)

            ### region most played
            try:
                most_played_region_content = fourth_content[1].findAll("a")
                most_played_region = []
                for el in most_played_region_content:
                    most_played_region.append(el.get_text().strip())

                artist_famous_locations.append(most_played_region)
            except:
                artist_famous_locations.append(None)

            ### club most played
            try:
                most_played_club_content = fourth_content[2].findAll("a")
                most_played_club = []
                for el in most_played_club_content:
                    most_played_club.append(parse.parse_qsl(parse.urlsplit(el.get("href")).query)[0][1])

                artist_famous_clubs.append(most_played_club)
            except:
                artist_famous_clubs.append(None)
        else:
            artist_collaborations.append(None)
            artist_famous_locations.append(None)
            artist_famous_clubs.append(None)



    data_artist_informations_return = pd.DataFrame({'DJ_name':list_artist_dj_names,'Name':artist_names, 'Origin':artist_basis_locations,
                                             'Online_account':online_urls,"aka":artist_aka, 'Followers':artist_popularities,
                                             'Description':artist_descriptions, 'Collaborations':artist_collaborations,
                                             'Famous_location':artist_famous_locations,
                                                    'Famous_clubs':artist_famous_clubs,
                                                    'url':list_url_artist, 'id':list_ids})

    return data_artist_informations_return


