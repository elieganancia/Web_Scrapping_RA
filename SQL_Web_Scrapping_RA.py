import mysql.connector
from unidecode import unidecode
import numpy as np

import API_Spotify_Web_Scrapping_RA as spot


def create_table_ra(db_filename):
    """Create a standard DataBase for any Resident_Advisor scrapping data
    :param db_filename : Name of the Database.
    :return: Create the Database in your current path
    """

    mydb = mysql.connector.connect(host="localhost", user="resident_advisor", passwd="bicep",
                                   auth_plugin='mysql_native_password')
    cur = mydb.cursor()

    cur.execute(""" CREATE DATABASE """ + db_filename)

    mydb = mysql.connector.connect(host="localhost", user="resident_advisor", db=db_filename, passwd="bicep",
                                   auth_plugin='mysql_native_password')
    cur = mydb.cursor()

    cur.execute("USE " + db_filename)

    cur.execute(
        '''CREATE TABLE artists (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                                 artist_id_ra VARCHAR(500),
                                 artist_url VARCHAR(500),
                                 artist_name VARCHAR(400)
                                 )''')

    cur.execute('''CREATE TABLE artists_information (
                                 id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                                 id_artist_ra varchar(500),
                                 artist_name varchar(500),
                                 artist_origin varchar(500),
                                 artist_social_media varchar(500),
                                 artist_nickname varchar(500),
                                 artist_follower INT,
                                 artist_description longtext,
                                 id_artist_collab varchar(500),
                                 artist_famous_location longtext,
                                 id_artist_most_played_club longtext,
                                 artist_genre longtext,
                                 artist_spotify_followers int,
                                 artist_spotify_url varchar(500),
                                 artist_image_url varchar(500)
                                   )''')

    cur.execute('''CREATE TABLE labels ( id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                                         id_label_ra varchar(500),
                                         url_label varchar(500)
                                           )''')

    cur.execute('''CREATE TABLE labels_information (
                                         id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                                         id_label_ra varchar(500),
                                         label_name varchar(500),
                                         label_creation int,
                                         label_country varchar(500),
                                         label_social_media varchar(500),
                                         label_follower int,
                                         label_description longtext,
                                         label_artist longtext
                                           )''')
    cur.execute('''CREATE TABLE countries (
                                         id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                                         id_country_ra varchar(500),
                                         country_name varchar(500)
                                           )''')

    cur.execute('''CREATE TABLE clubs_information (
                                         id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                                         id_country_ra varchar(500),
                                         club_id_ra varchar(500) ,
                                         club_name varchar(500),
                                         club_location varchar(500),
                                         club_follower int,
                                         club_phone varchar(500),
                                         club_capacity int,
                                         club_contact varchar(500)
                                                   )''')

    cur.execute('''CREATE TABLE events_information (
                                         id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                                         event_id_ra varchar(500),
                                         country_id_ra varchar(500),
                                         event_name varchar(500),
                                         event_date DATETIME,
                                         club_id_ra varchar(500),
                                         event_follower int,
                                         event_lineup longtext,
                                         event_artist longtext
                                                   )''')

    cur.execute('''CREATE TABLE events_meteo(
                                     id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                                     event_id_ra varchar(500),
                                     temperature float,
                                     humidity float,
                                     precipitation float,
                                     snow float
                                       )''')

    cur.execute('''CREATE TABLE artists_discography (
                                     id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                                     id_artist_ra varchar(500),
                                     name_artist varchar(500),
                                     album_name varchar(500),
                                     track_number int,
                                     track_name varchar(500),
                                     track_duration int,
                                     track_preview_url varchar(500)
                                       )''')
    cur.close()


def insert_artist(df, db_filename):
    """Insert artists into the database (Table Artist)
        :param df : Name of the Artist Dataframe.
        :param db_filename : Name of the Database
        :return: Insert in the db_filename the dataframe info
        """
    mydb = mysql.connector.connect(host="localhost", user="resident_advisor", db=db_filename, passwd="bicep",
                                   auth_plugin='mysql_native_password')
    cur = mydb.cursor()
    for i in range(len(df)):
        sql = "INSERT INTO artists (artist_id_ra,\
                                artist_url,\
                                artist_name)\
                        VALUES (%s,%s,%s)"

        val = (df["id"][i], unidecode(str(df["url"][i])), unidecode(str(df["name"][i])))
        cur.execute(sql, val)
    mydb.commit()
    cur.close()


def insert_artist_infos(df, db_filename):
    """Insert artists information into the database (Table Artist_Information)
        :param df : Name of the Artist Dataframe.
        :param db_filename : Name of the Database
        :return: Insert in the db_filename the datframe info
        """
    mydb = mysql.connector.connect(host="localhost", user="resident_advisor", db=db_filename, passwd="bicep",
                                   auth_plugin='mysql_native_password')
    cur = mydb.cursor()
    for i in range(len(df)):
        sql = "INSERT INTO artists_information (id_artist_ra,\
                            artist_name,\
                            artist_origin,\
                            artist_social_media,\
                            artist_nickname,\
                            artist_follower,\
                            artist_description,\
                            id_artist_collab,\
                            artist_famous_location,\
                            id_artist_most_played_club) \
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

        val = (unidecode(str(df["id"][i])),
               unidecode(str(df["Name"][i])),
               unidecode(str(df["Origin"][i])),
               unidecode(str(df["Online_account"][i])),
               unidecode(str(df["aka"][i])),
               np.int(df["Followers"][i]),
               unidecode(str(df["Description"][i])),
               unidecode(str(df["Collaborations"][i])),
               unidecode(str(df["Famous_location"][i])),
               unidecode(str(df["Famous_clubs"][i])))

        cur.execute(sql, val)
    mydb.commit()
    cur.close()


def insert_label(df, db_filename):
    """Insert Labels_information  into the database (Table label_information)
            :param df : Name of the Artist Information Dataframe.
            :param db_filename : Name of the Database
            :return: Insert in the db_filename the datframe info
            """
    mydb = mysql.connector.connect(host="localhost", user="resident_advisor", db=db_filename, passwd="bicep",
                                   auth_plugin='mysql_native_password')
    cur = mydb.cursor()
    for i in range(len(df)):
        sql = '''INSERT INTO labels_information(id_label_ra ,\
                                  label_name ,\
                                  label_creation ,\
                                  label_country ,\
                                  label_social_media ,\
                                  label_follower ,\
                                  label_description ,\
                                  label_artist) \
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)'''

        val = (unidecode(str(df["id"][i])),
               unidecode(str(df["Name"][i])),
               df["Creation"][i],
               unidecode(str(df["Country"][i])),
               unidecode(str(df["Online_account"][i])),
               np.int(df["Followers"][i]),
               unidecode(str(df["Description"][i])),
               unidecode(str(df["ids_artists"][i])))
        cur.execute(sql, val)
    mydb.commit()
    cur.close()


def insert_clubs(df, db_filename):
    """Insert Clubs Information into the database (Table clubs_information)
                :param df : Name of the Clubs Dataframe.
                :param db_filename : Name of the Database
                :return: Insert in the db_filename the dataframe info
    """
    mydb = mysql.connector.connect(host="localhost", user="resident_advisor", db=db_filename, passwd="bicep",
                                   auth_plugin='mysql_native_password')
    cur = mydb.cursor()
    for i in range(len(df)):
        sql = '''INSERT INTO clubs_information (id_country_ra ,\
                                  club_id_ra  ,\
                                  club_name ,\
                                  club_location ,\
                                  club_follower ,\
                                  club_phone ,\
                                  club_capacity ,\
                                  club_contact )\
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)'''

        val = (unidecode(str(df["Club_Country"][i])),
               unidecode(str(df["Club_ID"][i])),
               unidecode(str(df["Club_Name"][i])),
               unidecode(str(df["Club_Location"][i])),
               np.int(df["Club_Follower"][i]),
               unidecode(str(df["Club_Phone"][i])),
               np.int(df["Club_Capacity"][i]),
               unidecode(str(df["Club_Contact"][i])))
        cur.execute(sql, val)
    mydb.commit()
    cur.close()


def insert_events(df, db_filename):
    """Insert Events Information into the database (Table events_information)
                    :param df : Name of the Events Dataframe.
                    :param db_filename : Name of the Database
                    :return: Insert in the db_filename the dataframe info
    """
    mydb = mysql.connector.connect(host="localhost", user="resident_advisor", db=db_filename, passwd="bicep",
                                   auth_plugin='mysql_native_password')
    cur = mydb.cursor()
    for i in range(len(df)):
        sql = '''INSERT INTO events_information (event_id_ra ,\
                                      country_id_ra ,\
                                      event_name ,\
                                      event_date ,\
                                      club_id_ra ,\
                                      event_follower ,\
                                      event_lineup ,\
                                      event_artist) \
                                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'''

        val = (unidecode(str(df["Event_ID"][i])),
               unidecode(str(df["Country_ID"][i])),
               unidecode(str(df["Event_Name"][i])),
               df["Event_Date"][i],
               unidecode(str(df["Event_Location"][i])),
               np.int(df["Event_Follower"][i]),
               unidecode(str(df["Event_Lineup"][i])),
               unidecode(str(df["Event_Artists"][i])))
        cur.execute(sql, val)
    mydb.commit()
    cur.close()


def insert_artist_track(db_filename):
    """Insert Artists Tracks into the database (Table artists_discography) From the Spotify API
        (Add album names, track names, duration and preview of song..)
                        :param db_filename : Name of the Database
                        :return: Insert in the db_filename the dataframe info
        """
    mydb = mysql.connector.connect(host="localhost", user="resident_advisor", db=db_filename, passwd="bicep",
                                   auth_plugin='mysql_native_password')
    cur = mydb.cursor()
    cur.execute('''
        SELECT artist_name
        FROM artists''')
    result = cur.fetchall()
    for artist in result:
        print("Searching songs for ",artist[0],"...")
        df = spot.get_artist_song(artist[0])
        if df is not None:
            print("We found ",str(len(df))," songs... We are getting infos !")
            for j in range(len(df)):
                sql = '''INSERT INTO artists_discography (name_artist,\
                                              album_name,\
                                              track_number,\
                                              track_name ,\
                                              track_duration,\
                                              track_preview_url) \
                                              VALUES (%s, %s, %s, %s, %s, %s)'''

                val = (unidecode(str(df['artist_name'][j])),
                       unidecode(str(df['album_name'][j])),
                       np.int(df['track_number'][j]),
                       unidecode(str(df['track_name'][j])),
                       np.int(df['track_duration(ms)'][j]),
                       unidecode(str(df['track_preview_url'][j])))
                cur.execute(sql, val)
        else :
            print("We did'nt find any songs... I'm sorry ! Would you like a HUG ? ")
        mydb.commit()
    cur.close()


def update_artist_info(db_filename):
    """Update artists_information (Artist Genre, Followers, Images & Spotify URL) with the Spotify API
                            :param db_filename : Name of the Database
                            :return: Insert in the db_filename the API info
            """
    mydb = mysql.connector.connect(host="localhost", user="resident_advisor", db=db_filename, passwd="bicep",
                                   auth_plugin='mysql_native_password')
    cur = mydb.cursor()
    cur.execute('''
    SELECT artist_name, artist_id_ra
    FROM artists''')
    result = cur.fetchall()
    for artist in result:
        print("searchin info for :", artist)
        spotify_id = spot.get_artist_id(artist[0])
        artist_info = spot.get_artist_spotify_info(spotify_id)
        if artist_info is not None:
            genre = spot.get_artist_genres(artist_info)
            followers = spot.get_artist_followers(artist_info)
            image = spot.get_artist_thumbnail(artist_info)
            url = spot.get_artist_spotifyurl(artist_info)
            print("Launching Queries")
            cur.execute(
                '''UPDATE artists_information SET artist_genre = %s, artist_spotify_followers = %s, artist_spotify_url = %s, artist_image_url = %s where id_artist_ra = %s ''',
                [str(genre), np.int(followers), image, url, artist[1]])
            print("Artist Updated")
        mydb.commit()
    cur.close()


def database_check(DB_FILENAME):
    """Check if the Database exists
        :param db_filename : Name of the Database
    """
    mydb = mysql.connector.connect(host="localhost", user="resident_advisor", passwd="bicep",
                                   auth_plugin='mysql_native_password')
    cur = mydb.cursor()

    query = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = " + "'" + DB_FILENAME + "'"

    cur.execute(query)
    result = cur.fetchall()
    if len(result) == 0:
        print("The database does not exist ..... No worries")
        print("Scrappy Coco will creat one for you")
        create_table_ra(DB_FILENAME)
    else:
        print("The database already exists. Scrappy Coco will update it.")


def erase_database(DB_FILENAME):
    """Erase the Database
            :param db_filename : Name of the Database
        """

    mydb = mysql.connector.connect(host="localhost", user="resident_advisor", passwd="bicep",
                                   auth_plugin='mysql_native_password')
    cur = mydb.cursor()

    query = "DROP DATABASE " + DB_FILENAME

    cur.execute(query)
    cur.close()


def insert_meteo(df, db_filename):
    """Insert meteo into the table events_meteo from a meteo API
        :param db_filename : Name of the Database
        :return: Insert in the db_filename the dataframe info
            """
    mydb = mysql.connector.connect(host="localhost", user="resident_advisor", db=db_filename, passwd="bicep",
                                   auth_plugin='mysql_native_password')
    cur = mydb.cursor()
    for i in range(len(df)):
        sql = '''INSERT INTO events_meteo (event_id_ra,
                                    temperature ,
                                    humidity,
                                    precipitation,
                                    snow) \
                                    VALUES (%s, %s, %s, %s, %s)'''

        val = (str(df["event_id"][i]),
               float(df["temperature"][i]),
               float(df['humidity']),
               float(df['precipitation']),
               float(df['snow']))
        cur.execute(sql, val)
    mydb.commit()
    cur.close()
