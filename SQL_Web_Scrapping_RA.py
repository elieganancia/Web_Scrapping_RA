import sqlite3
import os
import mysql.connector


def create_table_ra(db_filename):
    """Create a standard DataBase for any Resident_Advisor scrapping data
    :param db_filename : Name of the Database.
    :return: Create the Database in your current path
    """

    mydb = mysql.connector.connect(host="localhost",user="resident_advisor",passwd="bicep",auth_plugin='mysql_native_password')
    cur = mydb.cursor()

    cur.execute(""" CREATE DATABASE """ + db_filename)

    mydb = mysql.connector.connect(host="localhost",user="resident_advisor", db = db_filename,passwd="bicep",auth_plugin='mysql_native_password')
    cur = mydb.cursor()

    cur.execute("USE " + db_filename)

    cur.execute("""CREATE TABLE artists (artist_id_ra INT PRIMARY KEY,artist_url VARCHAR(500),artist_name VARCHAR(400))""")

    cur.execute('''CREATE TABLE artists_information (
                                 id varchar(500) PRIMARY KEY,
                                 id_artist_ra varchar(500),
                                 artist_name varchar(500),
                                 artist_origin varchar(500),
                                 artist_social_media varchar(500),
                                 artist_nickname varchar(500),
                                 artist_follower int,
                                 artist_description varchar(500),
                                 id_artist_collab varchar(500),
                                 artist_famous_location varchar(500),
                                 id_artist_most_played_club int
                                   )''')

    cur.execute('''CREATE TABLE labels (
                                         id varchar(500) PRIMARY KEY,
                                         id_label_ra varchar(500),
                                         url_label varchar(500)
                                           )''')

    cur.execute('''CREATE TABLE labels_information (
                                         id varchar(500) PRIMARY KEY,
                                         id_label_ra varchar(500),
                                         label_name varchar(500),
                                         label_creation varchar(500),
                                         label_country varchar(500),
                                         label_social_media varchar(500),
                                         label_follower int,
                                         label_description varchar(500),
                                         label_artist varchar(500)
                                           )''')
    cur.execute('''CREATE TABLE countries (
                                         id varchar(500) PRIMARY KEY,
                                         id_country_ra varchar(500),
                                         country_name varchar(500)
                                           )''')

    cur.execute('''CREATE TABLE clubs_information (
                                         id varchar(500) PRIMARY KEY,
                                         id_country_ra varchar(500),
                                         club_id_ra int ,
                                         club_name varchar(500),
                                         club_location varchar(500),
                                         club_follower int,
                                         club_phone varchar(500),
                                         club_capacity int,
                                         club_contact varchar(500)
                                                   )''')

    cur.execute('''CREATE TABLE events_information (
                                         id varchar(500) PRIMARY KEY,
                                         event_id_ra int,
                                         country_id_ra varchar(500),
                                         event_name varchar(500),
                                         event_date varchar(500),
                                         club_id_ra int,
                                         event_follower int,
                                         event_lineup varchar(500),
                                         event_artist varchar(500)
                                                   )''')

    cur.close()



def insert_artist(df, db_filename):
    """Insert artists into the database (Table Artist_Information)
        :param df : Name of the Artist Dataframe.
        :param db_filename : Name of the Database
        :return: Insert in the db_filename the datframe info
        """
    mydb = mysql.connector.connect(host="localhost", user="resident_advisor", db=db_filename, passwd="bicep",
                                   auth_plugin='mysql_native_password')
    cur = mydb.cursor()
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
                     df["Famous_clubs"][i]
                     ])
    mydb.commit()
    cur.close()


def insert_label(df,db_filename):
    mydb = mysql.connector.connect(host="localhost", user="resident_advisor", db=db_filename, passwd="bicep",
                                   auth_plugin='mysql_native_password')
    cur = mydb.cursor()
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
                     df["ids_artists"][i]
                     ])
    mydb.commit()
    cur.close()


def insert_clubs(df,db_filename):
    mydb = mysql.connector.connect(host="localhost", user="resident_advisor", db=db_filename, passwd="bicep",
                                   auth_plugin='mysql_native_password')
    cur = mydb.cursor()
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
                     df["Club_Contact"][i]
                     ])
    mydb.commit()
    cur.close()


def insert_events(df, db_filename):
    mydb = mysql.connector.connect(host="localhost", user="resident_advisor", db=db_filename, passwd="bicep",
                                   auth_plugin='mysql_native_password')
    cur = mydb.cursor()
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
                     df["Event_Artists"][i]
                     ])
    mydb.commit()
    cur.close()

