from bs4 import BeautifulSoup
import requests
import pandas as pd
from urllib import parse
import time
import SQL_Web_Scrapping_RA as sqra
import datetime
from datetime import datetime
from Logger_Web_Scrapping_RA import Scrappy_logger
from Logger_Web_Scrapping_RA import Scrappy_info



def get_countries():
    """Web-Scrapping Countries from Resident Advisor to use for getting all events"""
    url_club = 'https://www.residentadvisor.net/events?show=all'
    with requests.Session() as res:
        country_page = res.get(url_club)
    soup_country = BeautifulSoup(country_page.content, 'html.parser')

    soup_country_ID = soup_country.findAll(class_="ulRegions")[0].find_all('a')
    country_ID = [soup_country_ID[i].get('href') for i in range(len(soup_country_ID))]
    soup_country_name = soup_country.findAll(class_="ulRegions")[0].find_all('a')
    country_name = [soup_country_name[i].get('title') for i in range(len(soup_country_name))]

    countries = list(zip(country_ID, country_name))
    return countries


def get_events(countries, db_filename, scrappy_info_, scrappy_log_):
    """
    This function get information on every event by country (or city) page
    :param countries: list containing id of countries (use function get_countries to scrap countries)
    :param countries: list containing id of countries (use function get_countries to scrap countries)
    :return: a dataframe containing specifics information for each events (name, country, date, location,
     followers, Line-up (list of dj by their RA id), Artist for the events (by name) )
    """


    events = pd.DataFrame()
    event = pd.DataFrame()
    scrappy_info_.info("Scrappy found {} countries. Scrappy will get all events available this "
                      "week for each country. \n".format(len(countries)))
    scrappy_log_.info("Scrappy found {} countries. Scrappy will get all events available this "
                      "week for each country. \n".format(len(countries)))
    for i in range(len(countries)):
        url_events = 'https://www.residentadvisor.net' + countries[i][0]
        with requests.Session() as res:
            club_page = res.get(url_events)
        soup_event = BeautifulSoup(club_page.content, 'html.parser')
        event_date, event_location_id, event_follower, event_lineup, event_artists = [], [], [], [], []

        event_id = [soup_event.findAll(class_="event-title")[i].find_all('a')[0].get('href').split('/')[-1] for i in
                    range(len(soup_event.findAll(class_="event-title")))]
        event_name = [soup_event.findAll(class_="event-title")[i].get_text() for i in
                      range(len(soup_event.findAll(class_="event-title")))]
        event_link = ['https://www.residentadvisor.net/events/' + id for id in event_id]
        for j, link in enumerate(event_link):
            with requests.Session() as res:
                club_page = res.get(link)
            time.sleep(0.1)
            soup_event_page = BeautifulSoup(club_page.content, 'html.parser')
            try:
                date =soup_event_page.findAll(class_="cat-rev")[0].get_text()
                event_date.append(datetime.datetime.strptime(date, '%d %b %Y'))
            except:
                event_date.append(datetime.datetime(1,1,1,1,1))
            try:
                event_location_id.append(
                    parse.parse_qsl(parse.urlsplit(soup_event_page.findAll(class_="cat-rev")[1].get('href')).query)[0][
                        1])
            except:
                event_location_id.append('None')
            try:
                event_follower.append(int(soup_event.findAll(id="MembersFavouriteCount")[0].get_text().split('\n')[1]))
            except:
                event_follower.append(int(0))
            try:
                event_lineup.append(soup_event_page.findAll(class_="lineup large")[0].get_text())
            except:
                event_lineup.append('None')
            try:
                event_artists.append(
                    [soup_event_page.findAll(class_="lineup large")[0].find_all('a')[i].get('href').split('/')[-1] for i
                     in range(len(soup_event_page.findAll(class_="lineup large")[0].find_all('a')))])
            except:
                event_artists.append('None')
            country_link = [countries[i][1]] * len(event_artists)
            event_list = list(
                zip(country_link, event_id, event_link, event_name, event_date, event_location_id, event_follower,
                    event_lineup, event_artists))
            event = pd.DataFrame(event_list,
                                 columns=['Country_ID', 'Event_ID', 'Event_Link', 'Event_Name', 'Event_Date',
                                          'Event_Location', 'Event_Follower', 'Event_Lineup', 'Event_Artists'])
            scrappy_log_.info(str(j) + ' Scrapping Event Page from ' + countries[i][0] + ' : ' + link)
        events = events.append(event, ignore_index=True)

        if len(events) > 1000:
            sqra.insert_events(events, db_filename)
            events = pd.DataFrame()
            scrappy_info_.info("Commiting Database...........")
    if len(events) > 0:
        sqra.insert_events(events, db_filename)
        scrappy_info_.info("Commiting Database...........")
    return events


def get_countries_id(scrappy_info_, scrappy_log_):
    """Web-Scrapping Countries from Resident Advisor to use for getting all clubs"""

    scrappy_info_.info("Scrappy is getting a listinf of each countries of RA")
    scrappy_log_.info("Scrappy is getting a listinf of each countries of RA")
    city_name = []
    city_id = []
    url_club = 'https://www.residentadvisor.net/clubs.aspx?ai=44'
    with requests.Session() as res:
        country_page = res.get(url_club)
        soup_country = BeautifulSoup(country_page.content, 'html.parser')
    country_id = [soup_country.findAll(class_='links')[0].findAll('a')[i].get('href') for i in
                  range(len(soup_country.findAll(class_='links')[0].findAll('a')))]
    country_name = [soup_country.findAll(class_='links')[0].findAll('a')[i].get('href') for i in
                    range(len(soup_country.findAll(class_='links')[0].findAll('a')))]
    countries = list(zip(country_name, country_id))
    for country in countries:
        url_club = 'https://www.residentadvisor.net' + country[1]
        with requests.Session() as res:
            country_page = res.get(url_club)
            soup_country = BeautifulSoup(country_page.content, 'html.parser')
        city_name.append([soup_country.findAll(class_='links')[1].findAll('a')[i].get_text() for i in
                          range(len(soup_country.findAll(class_='links')[1].findAll('a')))])
        city_id += [soup_country.findAll(class_='links')[1].findAll('a')[i].get('href') for i in
                    range(len(soup_country.findAll(class_='links')[1].findAll('a')))]
    return city_id


def get_clubs(countries_id, db_filename, scrappy_info_, scrappy_log_):
    """
    This function get information on every clubs by country (or city) page
    :param countries_id: list containing RA id of countries (use function get_countries_id to scrap countries id)
    :return: a dataframe containing specifics information for each club (name, country, id, location,
     followers, capacity, phone , contact )
    """

    clubs = pd.DataFrame()
    scrappy_log_.info("Scrappy is starting the scrapping of club page")
    scrappy_info_.info("Scrappy is starting the scrapping of club page")
    for country_id in countries_id:
        time.sleep(0.1)
        url_club = 'https://www.residentadvisor.net' + country_id
        with requests.Session() as res:
            club_page = res.get(url_club)
        soup = BeautifulSoup(club_page.content, 'html.parser')
        club_name_htm = soup.findAll(class_='fl col4-6')[0].findAll('a')
        club_name = [club_name_htm[i].get_text().title() for i in range(len(club_name_htm))]
        club_loc_htm = soup.findAll(class_="fl grey mobile-off")
        club_loc = [club_loc_htm[i].get_text() for i in range(len(club_loc_htm))]
        club_id_htm = soup.findAll(class_='fl col4-6')[0].findAll('a', href=True, text=True)
        club_id = [club_name_htm[i].get('href') for i in range(len(club_id_htm))]
        club_id = [parse.parse_qsl(parse.urlsplit(club_id[i]).query)[0][1] for i in range(len(club_id_htm))]
        club_ra_link = ['https://www.residentadvisor.net/club.aspx?id=' + id for id in club_id]
        club_followers = []
        club_contact = []
        club_phone = []
        club_capacity = []
        for i in range(len(club_ra_link)):
            time.sleep(0.1)
            with requests.Session() as res:
                club_page = res.get(club_ra_link[i])
            page_soup = BeautifulSoup(club_page.content, 'html.parser')
            phone_ = False
            capacity_ = False
            for el in page_soup.findAll(class_="fl col4-6 small")[0].findAll(class_="clearfix")[1].findAll('li'):
                if "phone" in el.get_text().lower():
                    club_phone.append(el.get_text().split('/')[-1])
                    phone_ = True
                if "capacity" in el.get_text().lower():
                    club_capacity.append(int(el.get_text().split('/')[-1]))
                    capacity_ = True
            if not phone_:
                club_phone.append("None")
            if not capacity_:
                club_capacity.append(int(0))
            try:
                club_followers.append(int(page_soup.findAll(class_="favCount")[0].get_text().strip()))
            except:
                club_followers.append(int(0))
            try:
                club_contact.append(
                    [page_soup.findAll(class_="fl col4-6 small")[0].find_all('a', href=True, text=True)[i].get('href')
                     for i
                     in range(len(page_soup.findAll(class_="fl col4-6 small")[0].find_all('a', href=True, text=True)))][
                        0])
            except:
                club_contact.append('None')
            scrappy_log_.info('Scrapping Club Page : ' + club_name[i].title())
        club = pd.DataFrame(
            {'Club_Country': country_id, 'Club_link': club_ra_link, 'Club_ID': club_id,
             'Club_Name': club_name, 'Club_Location': club_loc,
             'Club_Follower': club_followers, 'Club_Phone': club_phone,
             'Club_Capacity': club_capacity, 'Club_Contact': club_contact})
        clubs = clubs.append(club, ignore_index=True)
        if len(clubs) > 1000:
            sqra.insert_clubs(clubs, db_filename)
            clubs = pd.DataFrame()
            scrappy_info_.info("Commiting Database...........")
    if len(clubs) > 0:
        sqra.insert_clubs(clubs, db_filename)
    return clubs
