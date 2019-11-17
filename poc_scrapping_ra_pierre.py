
##################
###  Library  ####
##################

from bs4 import BeautifulSoup
import requests
from urllib import parse
import pandas as pd


pd.set_option('display.max_columns', 500)

###################
###  Utilities  ###
###################

def get_content(url_):
    with requests.Session() as res:
        page_ = res.get(url_)

    soup_return = BeautifulSoup(page_.content, 'html.parser')

    return soup_return


################################################################################################################
###############################     Scrapping     ##############################################################
################################################################################################################

base_url = "https://www.residentadvisor.net"

#######################
### Label Database ####
#######################


##### Get list of labels
url_labels = "https://www.residentadvisor.net/labels.aspx?show=all"


def get_labels(url_labels_):
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
    data_labels_return['url'] = BASE_URL + data_labels_return['url']

    return data_labels_return


data_labels = get_labels(url_labels)

######---> specific labels


def get_label_information(data_labels_):
    list_url_label = list(data_labels_['url'])
    list_name_label = list(data_labels_['name'])

    date_creation_labels = []
    city_labels = []
    country_labels = []
    online_urls = []
    label_popularities = []
    label_description = []

    for url_ in list_url_label:
        label_information_content = get_content(url_)

        ### first type of informtion
        first_content = label_information_content.findAll(class_="fl col4-6 small")[0].findAll(class_="clearfix")[1]
        content_list = first_content.findAll('li')

        ## data established
        date_creation_labels.append(content_list[0].get_text()[-4:])

        ## location

        city_labels.append(content_list[1].findAll('a')[0].get_text().strip().split(",")[0])
        country_labels.append(content_list[1].findAll('a')[0].get_text().strip().split(",")[0])

        ## online link

        label_urls = []
        list_of_urls = content_list[2].findAll('a')
        for el in list_of_urls:
            label_urls.append(el.get('href'))

        online_urls.append(list_of_urls)

        ### second type of information (number of follower)
        second_content = label_information_content.findAll(class_="fav button clearfix")
        label_popularities.append(second_content[0].findAll(id="MembersFavouriteCount")[0].get_text().strip().replace(",", ""))

        ### third type of information (description)
        third_content = label_information_content.findAll(class_="record-label-blurb")
        label_description.append(third_content[0].get_text())

    data_label_information_return = pd.DataFrame({'Name':list_name_label, 'Creation':date_creation_labels,
                                                  'Country':country_labels, 'Online_account':online_urls,
                                                  'Followers':label_popularities, 'Description': label_description})

    return data_label_information_return


data_labels_information = get_label_information(data_labels)

########################
### Artist Database ####
########################

url_artists = "https://www.residentadvisor.net/dj.aspx"


def get_artists(url_artists_):
    artist_list_content = get_content(url_artists_)
    list_letters = artist_list_content.findAll(class_='fl pr8')

    artist_names = []
    artist_urls = []
    artist_ids = []
    #we loop first over all letters
    for i in range(len(list_letters)):
        # we get liste of label for each letters
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

    return data_artist_return


data_artists = get_artists(url_artists)


#---> specific artist

def get_artist_information(data_artist_):

    list_url_artist = list(data_artist_['url'])

    artist_names = []
    artist_basis_locations = []
    online_urls = []

    artist_popularities = []
    artist_descriptions = []
    artist_collaborations = []
    artist_famous_locations = []
    artist_famous_clubs = []

    for url_ in list_url_artist:
        artist_information_content = get_content(url_)

        # First type of Information (names/location/online account)
        first_content = artist_information_content.findAll(class_="col4-6 small fl")[0].findAll(class_="clearfix")[1]
        content_list = first_content.findAll('li')
        artist_names.append(content_list[0].get_text().split("/")[-1])
        artist_basis_locations.append(content_list[1].findAll("a")[0].get_text().strip())

        artist_urls = []
        list_of_urls = content_list[2].findAll('a')
        for el in list_of_urls:
            artist_urls.append(el.get('href'))

        online_urls.append(list_of_urls)

        # Second type of information (popularity)

        second_content = artist_information_content.findAll(class_="fav button clearfix")
        artist_popularities.append(second_content.findAll(id="MembersFavouriteCount")[0].get_text().strip().replace(",",""))

        # Third type of information (description)
        third_content = artist_information_content.findAll(class_="excerpt mobile-pr24-tablet-desktop-pr8 pt8")
        artist_descriptions.append(third_content[0].get_text())

        # Fourth type of information :
        fourth_content_temp = artist_information_content.findAll(class_="countstats stats")[0]
        fourth_content = fourth_content_temp.findAll(class_="stats-list list clearfix")[0].findAll("li")

        ### Appears most with
        most_played_artist_with = []
        most_played_artist_with_content = fourth_content[0].findAll("a")
        for el in most_played_artist_with_content:
            most_played_artist_with.append(el.get("href").split("/")[-1])

        artist_collaborations.append(most_played_artist_with)

        ### region most played
        most_played_region_content = fourth_content[1].findAll("a")
        most_played_region = []
        for el in most_played_region_content:
            most_played_region.append(el.get_text().strip())

        artist_famous_locations.append(most_played_region)

        ### club most played
        most_played_club_content = fourth_content[2].findAll("a")
        most_played_club = []
        for el in most_played_club_content:
            most_played_club.append(parse.parse_qsl(parse.urlsplit(el.get("href")).query)[0][1])

        artist_famous_clubs.append(most_played_club)


    data_artist_informations_return = pd.DataFrame({'Name':artist_names, 'Origin':artist_basis_locations,
                                             'Online_account':online_urls, 'Followers':artist_popularities,
                                             'Description':artist_descriptions, 'Collaborations':artist_collaborations,
                                             'Famous_location':artist_famous_locations,
                                                    'Famous_clubs':artist_famous_clubs})

    return data_artist_informations_return


data_artists_information = get_artist_information(data_artists)

########################################################################################################################
# monday
#commit main
# one submit hive + github
