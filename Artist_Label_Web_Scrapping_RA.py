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
import SQL_Web_Scrapping_RA as ra_sql


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

def get_labels(url_labels_,scrappy_info_, scrappy_log_):
    """
    This function get basic information (name/url/id) of all the labels on Resident advisor
    The urls will be used to get information for each label
    :param url_labels_: url of the page where to get these basic information
    :return: a pandas dataframe which contains these basic information
    """


    scrappy_info_.info("//////////////////////////////////////////////////////////////////////////////////////////")
    scrappy_info_.info("      Scrappy Coco is getting is listing all labels of Resident Advisor")
    scrappy_info_.info("////////////////////////////////////////////////////////////////////////////////////////// \n")

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
    scrappy_info_.info("{0} labels have been found on Resident Advisor \n".format(data_labels_return.shape[0]))
    data_labels_return['url'] = BASE_URL + data_labels_return['url']

    return data_labels_return


######---> specific labels


def get_label_information(data_labels_, DB_FILENAME,scrappy_info_, scrappy_log_):
    """
    This function get information on each labels url page
    :param data_labels_: dataframe cotaining url pages of labels
    :return: a dataframe containing specifics information for each artists (name, date, country, online account,
     followers and description)
    """


    list_url_label = list(data_labels_['url'])

    scrappy_info_.info("//////////////////////////////////////////////////////////////////////////////////////////")
    scrappy_info_.info("      Scrappy Coco is getting information for all labels ({0} labels)".format(len(list_url_label)))
    scrappy_info_.info("////////////////////////////////////////////////////////////////////////////////////////// \n")

    scrappy_log_.info("Scrappy Coco is getting information for all labels ({0} labels)".format(len(list_url_label)))
    list_name_label = []
    list_ids_label = []
    date_creation_labels = []
    location_labels = []
    online_urls = []
    label_popularities = []
    label_description = []
    label_artists = []


    for ind_, url_ in enumerate(list_url_label):
        scrappy_log_.info("Getting information on : {0}".format(url_))
        label_information_content = get_content(url_)

        list_name_label.append(data_labels_['name'][ind_])
        list_ids_label.append(data_labels_['id'][ind_])

        ### first type of information
        first_content = label_information_content.findAll(class_="fl col4-6 small")[0].findAll(class_="clearfix")[1]
        content_list = first_content.findAll('li')

        date_ = False
        location_label_ = False
        online_ = False

        for el in content_list:
            if "established" in el.find("div").get_text().lower():
                try:
                    date_creation_labels.append(int(el.get_text()[-4:]))
                except:
                    date_creation_labels.append(0)
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
            date_creation_labels.append(0)
        if not location_label_:
            location_labels.append(None)
        if not online_:
            online_urls.append(None)

        ### second type of information (number of follower)
        second_content = label_information_content.findAll(class_="fav button clearfix")
        try:
            label_popularities.append(
                int(second_content[0].findAll(id="MembersFavouriteCount")[0].get_text().strip().replace(",", "")))
        except:
            label_popularities.append(0)


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

        if len(label_artists) > 100:

            data_label_information_return = pd.DataFrame({'Name': list_name_label, 'Creation': date_creation_labels,
                                                          'Country': location_labels, 'Online_account': online_urls,
                                                          'Followers': label_popularities,
                                                          'Description': label_description,
                                                          'id': list_ids_label, "ids_artists": label_artists})
            scrappy_info_.info("Committing Database ....")
            ra_sql.insert_label(data_label_information_return, DB_FILENAME)

            list_name_label = []
            list_ids_label = []
            date_creation_labels = []
            location_labels = []
            online_urls = []
            label_popularities = []
            label_description = []
            label_artists = []


    data_label_information_return = pd.DataFrame({'Name': list_name_label, 'Creation': date_creation_labels,
                                                  'Country': location_labels, 'Online_account': online_urls,
                                                  'Followers': label_popularities, 'Description': label_description,
                                                  'id': list_ids_label,"ids_artists": label_artists})
    scrappy_info_.info("Committing Database ....")
    ra_sql.insert_label(data_label_information_return, DB_FILENAME)
    scrappy_info_.info("\n")

    return data_label_information_return



########################
### Artist Database ####
########################


def get_artists(url_artists_, scrappy_info_, scrappy_log_):
    """
    This function get basic information (name/url/id) of all the artists on Resident advisor
    The urls will be used to get information for each label
    :param url_artists_: url of the page where to get these basic information
    :return: a pandas dataframe which contains these basic information
    """

    scrappy_info_.info("//////////////////////////////////////////////////////////////////////////////////////////")
    scrappy_info_.info("          Scrappy is getting a listing of all artists of Resident Advisor")
    scrappy_info_.info("//////////////////////////////////////////////////////////////////////////////////////////\n")

    scrappy_log_.info("Scrappy is getting all artists of Resident Advisor")

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
    scrappy_info_.info("{0} artists have been found on Resident Advisor".format(data_artist_return.shape[0]))
    scrappy_log_.info("{0} artists have been found on Resident Advisor".format(data_artist_return.shape[0]))

    return data_artist_return


# specific artist

def get_artist_information(data_artist_, DB_FILENAME, scrappy_info_, scrappy_log_):
    """
    This function get information on each artist url page
    :param data_artist_: dataframe cotaining url pages of artists
    :return: a dataframe containing specifics information for each artists (name, country, online account, alliases,
     followers, description, collaboration, famous location, famous club , url & ids)
    """


    list_url_artist = list(data_artist_['url'])

    scrappy_info_.info("//////////////////////////////////////////////////////////////////////////////////////////")
    scrappy_info_.info("The script is getting information for all artists ({0} artists)".format(len(list_url_artist)))
    scrappy_info_.info("////////////////////////////////////////////////////////////////////////////////////////// \n")

    scrappy_log_.info("The script is getting information for all artists ({0} artists)".format(len(list_url_artist)))

    artist_names = []
    artist_basis_locations = []
    online_urls = []
    list_url_artist_return = []
    artist_popularities = []
    artist_descriptions = []
    artist_collaborations = []
    artist_famous_locations = []
    artist_famous_clubs = []
    artist_aka = []
    list_artist_dj_names = []
    list_ids = []

    for ind_, url_ in enumerate(list_url_artist):

        list_url_artist_return.append(url_)
        list_ids.append(data_artist_['id'][ind_])
        list_artist_dj_names.append(data_artist_['name'][ind_])


        artist_information_content = get_content(url_)
        scrappy_log_.info("Getting information on : {0}".format(url_))

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
        try:
            artist_popularities.append(int(second_content.findAll(id="MembersFavouriteCount")[0].get_text().strip().replace(",","")))
        except:
            artist_popularities.append(0)

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

        if len(artist_basis_locations)>100:

            scrappy_info_.info("Committing Database ....")
            data_artist_informations_return = pd.DataFrame(
                {'DJ_name': list_artist_dj_names, 'Name': artist_names, 'Origin': artist_basis_locations,
                 'Online_account': online_urls, "aka": artist_aka, 'Followers': artist_popularities,
                 'Description': artist_descriptions, 'Collaborations': artist_collaborations,
                 'Famous_location': artist_famous_locations,
                 'Famous_clubs': artist_famous_clubs,
                 'url': list_url_artist_return, 'id': list_ids})

            ra_sql.insert_artist_infos(data_artist_informations_return, DB_FILENAME)

            artist_names = []
            artist_basis_locations = []
            online_urls = []
            list_url_artist_return = []
            artist_popularities = []
            artist_descriptions = []
            artist_collaborations = []
            artist_famous_locations = []
            artist_famous_clubs = []
            artist_aka = []
            list_artist_dj_names = []
            list_ids = []


    data_artist_informations_return = pd.DataFrame({'DJ_name':list_artist_dj_names,'Name':artist_names, 'Origin':artist_basis_locations,
                                             'Online_account':online_urls,"aka":artist_aka, 'Followers':artist_popularities,
                                             'Description':artist_descriptions, 'Collaborations':artist_collaborations,
                                             'Famous_location':artist_famous_locations,
                                                    'Famous_clubs':artist_famous_clubs,
                                                    'url':list_url_artist_return, 'id':list_ids})
    ra_sql.insert_artist_infos(data_artist_informations_return, DB_FILENAME)
    scrappy_info_.info("Committing Database ....")

    return data_artist_informations_return



