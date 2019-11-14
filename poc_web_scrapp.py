from bs4 import BeautifulSoup
import requests
import pandas as pd
from urllib import parse


"""Web-Scrapping Countries from Resident Advisor"""


url_club = 'https://www.residentadvisor.net/events?show=all'
with requests.Session() as res :
    country_page = res.get(url_club)
soup_country = BeautifulSoup(country_page.content, 'html.parser')
country_ID = [soup_country.findAll(class_="ulRegions")[0].find_all('a')[i].get('href') for i in range(len(soup_country.findAll(class_="ulRegions")[0].find_all('a')))]
country_name = [soup_country.findAll(class_="ulRegions")[0].find_all('a')[i].get('title') for i in range(len(soup_country.findAll(class_="ulRegions")[0].find_all('a')))]
countries = list(zip(country_ID,country_name))
"""Web-Scrapping Events from Resident Advisor"""
events = pd.DataFrame()
for i in range(len(countries)) :
    url_events = 'https://www.residentadvisor.net' + countries[i][0]
    with requests.Session() as res :
        club_page = res.get(url_events)
    soup_event = BeautifulSoup(club_page.content, 'html.parser')
    event_id = [soup_event.findAll(class_="event-title")[i].find_all('a')[0].get('href').split('/')[-1] for i in range(len(soup_event.findAll(class_="event-title")))]
    event_name = [soup_event.findAll(class_="event-title")[i].get_text() for i in range(len(soup_event.findAll(class_="event-title"))) ]
    event_link = ['https://www.residentadvisor.net/events/' + id for id in event_id]
    event_date = []
    event_location_id = []
    event_follower = []
    event_lineup = []
    event_artists = []
    for link in event_link :
        with requests.Session() as res :
            club_page = res.get(link)
        soup_event_page = BeautifulSoup(club_page.content, 'html.parser')
        try :
            event_date.append(soup_event_page.findAll(class_="cat-rev")[0].get_text())
        except :
            event_date.append('None')
        try:
            event_location_id.append(parse.parse_qsl(parse.urlsplit(soup_event_page.findAll(class_="cat-rev")[1].get('href')).query)[0][1])
        except :
            event_location_id.append('None')
        try:
            event_follower.append(soup_event_page.findAll(id="MembersFavouriteCount")[0].get_text())
        except :
            event_follower.append('None')
        try :
            event_lineup.append(soup_event_page.findAll(class_="lineup large")[0].get_text())
        except :
            event_lineup.append('None')
        try :
            event_artists.append([soup_event_page.findAll(class_="lineup large")[0].find_all('a')[i].get('href').split('/')[-1] for i in range(len(soup_event_page.findAll(class_="lineup large")[0].find_all('a')))])
        except :
            event_artists.append('None')

        country_link = [countries[i][1]]*len(event_artists)
        event_list = list(zip(country_link, event_id, event_link,event_name, event_date, event_location_id, event_follower, event_lineup,event_artists))
        event = pd.DataFrame(event_list, columns = ['Country_ID','Event_ID', 'Event_Link','Event_Name', 'Event_Date', 'Event_Location', 'Event_Follower', 'Event_Lineup','Event_Artists'])
    events = events.append(event, ignore_index=True)


"""Web-Scrapping Clubs from Resident Advisor"""


url_club = 'https://www.residentadvisor.net/club.aspx'
with requests.Session() as res :
    club_page = res.get(url_club)
soup = BeautifulSoup(club_page.content, 'html.parser')

club_name_htm = soup.findAll(class_='fl col4-6')[0].findAll('a')
club_name = [club_name_htm[i].get_text() for i in range(len(club_name_htm))]
club_loc_htm = soup.findAll(class_="fl grey mobile-off")
club_loc = [club_loc_htm[i].get_text() for i in range(len(club_loc_htm))]
club_id_htm = soup.findAll(class_='fl col4-6')[0].findAll('a',href = True,text=True)
club_id = [club_name_htm[i].get('href') for i in range(len(club_id_htm))]
club_id = [parse.parse_qsl(parse.urlsplit(club_id[i]).query)[0][1] for i in range(len(club_id_htm))]
club_RA_link = ['https://www.residentadvisor.net/club.aspx?id='+id for id in club_id]
club_followers = []
club_email = []

for url_club in club_RA_link :
    with requests.Session() as res :
        club_page = res.get(url_club)
    page_soup = BeautifulSoup(club_page.content, 'html.parser')
    try :
        club_followers.append(page_soup.findAll(class_="favCount")[0].get_text().strip())
    except :
        club_followers.append('None')
    try :
        club_email.append(page_soup.findAll(class_="wide")[1].find_all('a', href=True, text=True)[0].get('href'))
    except :
        club_email.append('None')
    try :
        club_email.append(page_soup.findAll(class_="pr24 pt8")[1].find_all('a', href=True, text=True)[0].get('href'))
    except :
        club_email.append('None')

club_list = list(zip(club_RA_link, club_id, club_name, club_loc, club_followers, club_email))
club = pd.DataFrame(club_list, columns = ['Club_link', 'Club_ID', 'Club_Name', 'Club_Location', 'Club_Follower', 'Club_Email'])


