import Web_Scrapping_RA.poc_scrapping_ra_pierre as sp
import Web_Scrapping_RA.poc_web_scrapp as se


def main():
    url_labels = "https://www.residentadvisor.net/labels.aspx?show=all"
    data_labels = sp.get_labels(url_labels)

    data_labels_information = sp.get_label_information(data_labels)


    url_artists = "https://www.residentadvisor.net/dj.aspx"
    data_artists = sp.get_artists(url_artists)

    data_artists_information = sp.get_artist_information(data_artists)


    data_countries = se.get_countries()
    data_events = se.get_events(data_countries)
    data_clubs = se.get_clubs()


if __name__ == '__main__':
    main()