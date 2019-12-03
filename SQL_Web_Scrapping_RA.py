import sqlite3
import os
import pandas as pd


def create_table_ra(db_filename):

    if os.path.exists(db_filename):
        os.remove(db_filename)
    with sqlite3.connect(db_filename) as con:
        cur = con.cursor()
        cur.execute('''CREATE TABLE artists (
                        artist_id_ra int PRIMARY KEY,
                        artist_url varchar,
                        artist_name varchar,
                        ) ''')

        cur.execute('''CREATE TABLE artists_information (
                          id varchar PRIMARY KEY,
                          id_artist_ra varchar,
                          artist_name varchar ,
                          artist_origin varchar,
                          artist_social_media varchar,
                          artist_nickname varchar,
                          artist_follower int,
                          artist_description varchar,
                          id_artist_collab varchar,
                          artist_famous_location varchar,
                          id_artist_most_played_club int,
                            )''')

        cur.execute('''CREATE TABLE labels (
                                  id varchar PRIMARY KEY,
                                  id_label_ra varchar,
                                  url_label varchar,
                                    )''')

        cur.execute('''CREATE TABLE labels_information (
                                  id varchar PRIMARY KEY,
                                  id_label_ra varchar,
                                  label_name varchar,
                                  label_creation varchar,
                                  label_country varchar,
                                  label_social_media varchar,
                                  label_follower int,
                                  label_description varchar,
                                  label_artist varchar,
                                    )''')
        cur.execute('''CREATE TABLE countries (
                                  id varchar PRIMARY KEY,
                                  id_country_ra varchar,
                                  country_name varchar,
                                    )''')

        cur.execute('''CREATE TABLE clubs_information (
                                  id varchar PRIMARY KEY,
                                  id_country_ra varchar,
                                  club_id_ra int ,
                                  club_name varchar,
                                  club_location varchar,
                                  club_follower int,
                                  club_phone varchar,
                                  club_capacity int,
                                  club_contact varchar
                                            )''')

        cur.execute('''CREATE TABLE events_information (
                                  id varchar PRIMARY KEY,
                                  event_id_ra int,
                                  country_id_ra varchar,
                                  event_name varchar,
                                  event_date varchar,
                                  club_id_ra int,
                                  event_follower int,
                                  event_lineup varchar,
                                  event_artist varchar,
                                            )''')

        con.commit()
        cur.close()


db_filename = 'Web_Scrapping_RA.db'


def insert_artist(df):
    with sqlite3.connect(db_filename) as con:
        cur = con.cursor()
    for i in range(len(df)) :
        cur.execute("INSERT INTO artists_information (id ,\
        id_artist_ra,\
        artist_name,\
        artist_origin,\
        artist_social_media,\
        artist_nickname,\
        artist_follower,\
        artist_description,\
        id_artist_collab,\
        artist_famous_location,\
        id_artist_most_played_club ) \
                        VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    [i,
                     df["id"][i],
                     df["Name"][i],
                     df["Origin"][i],
                     df["Online_account"][i],
                     df["aka"][i],
                     df["Followers"][i],
                     df["Description"][i],
                     df["Collaborations"][i],
                     df["Famous_location"][i],
                     df["Famous_clubs"][i],
                     ])
    con.commit()
    cur.close()


def insert_label(df):
    with sqlite3.connect(db_filename) as con:
        cur = con.cursor()
    for i in range(len(df)) :
        cur.execute("INSERT INTO label_information (id ,\
                                  id_label_ra ,\
                                  label_name ,\
                                  label_creation ,\
                                  label_country ,\
                                  label_social_media ,\
                                  label_follower ,\
                                  label_description ,\
                                  label_artist) \
                        VALUES (?,?,?,?,?,?,?,?,?)",
                    [i,
                     df["id"][i],
                     df["Name"][i],
                     df["Creation"][i],
                     df["Country"][i],
                     df["Online_account"][i],
                     df["Followers"][i],
                     df["Description"][i],
                     df["ids_artists"][i],
                     ])
    con.commit()
    cur.close()


def insert_clubs(df):
    with sqlite3.connect(db_filename) as con:
        cur = con.cursor()
    for i in range(len(df)) :
        cur.execute("INSERT INTO artists_information (id ,\
                                  id_country_ra ,\
                                  club_id_ra  ,\
                                  club_name ,\
                                  club_location ,\
                                  club_follower ,\
                                  club_phone ,\
                                  club_capacity ,\
                                  club_contact )\
                        VALUES (?,?,?,?,?,?,?,?,?)",
                    [i,
                     df["Club_Country"][i],
                     df["Club_ID"][i],
                     df["Club_Name"][i],
                     df["Club_Location"][i],
                     df["Club_Follower"][i],
                     df["Club_Phone"][i],
                     df["Club_Capacity"][i],
                     df["Club_Contact"][i],
                     ])
    con.commit()
    cur.close()


def insert_events(df):
    with sqlite3.connect(db_filename) as con:
        cur = con.cursor()
    for i in range(len(df)) :
        cur.execute("INSERT INTO artists_information (id ,\
                                  event_id_ra ,\
                                  country_id_ra ,\
                                  event_name ,\
                                  event_date ,\
                                  club_id_ra ,\
                                  event_follower ,\
                                  event_lineup ,\
                                  event_artist ,\
                        VALUES (?,?,?,?,?,?,?,?,?)",
                    [i,
                     df["Event_ID"][i],
                     df["Country_ID"][i],
                     df["Event_Name"][i],
                     df["Event_Date"][i],
                     df["Event_Location"][i],
                     df["Event_Follower"][i],
                     df["Event_Lineup"][i],
                     df["Event_Artists"][i],
                     ])
    con.commit()
    cur.close()
