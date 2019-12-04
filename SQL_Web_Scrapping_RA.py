import mysql.connector
from unidecode import unidecode


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
        """CREATE TABLE artists (artist_id_ra INT PRIMARY KEY,artist_url VARCHAR(500),artist_name VARCHAR(400))""")

    cur.execute('''CREATE TABLE artists_information (
                                 id int PRIMARY KEY,
                                 id_artist_ra varchar(500),
                                 artist_name varchar(500),
                                 artist_origin varchar(500),
                                 artist_social_media varchar(500),
                                 artist_nickname varchar(500),
                                 artist_follower varchar(500),
                                 artist_description varchar(500),
                                 id_artist_collab varchar(500),
                                 artist_famous_location varchar(500),
                                 id_artist_most_played_club varchar(500)
                                   )''')

    cur.execute('''CREATE TABLE labels (
                                         id int PRIMARY KEY,
                                         id_label_ra varchar(500),
                                         url_label varchar(500)
                                           )''')

    cur.execute('''CREATE TABLE labels_information (
                                         id int PRIMARY KEY,
                                         id_label_ra varchar(500),
                                         label_name varchar(500),
                                         label_creation varchar(500),
                                         label_country varchar(500),
                                         label_social_media varchar(500),
                                         label_follower varchar(500),
                                         label_description varchar(500),
                                         label_artist varchar(500)
                                           )''')
    cur.execute('''CREATE TABLE countries (
                                         id int PRIMARY KEY,
                                         id_country_ra varchar(500),
                                         country_name varchar(500)
                                           )''')

    cur.execute('''CREATE TABLE clubs_information (
                                         id int PRIMARY KEY,
                                         id_country_ra varchar(500),
                                         club_id_ra varchar(500) ,
                                         club_name varchar(500),
                                         club_location varchar(500),
                                         club_follower varchar(500),
                                         club_phone varchar(500),
                                         club_capacity varchar(500),
                                         club_contact varchar(500)
                                                   )''')

    cur.execute('''CREATE TABLE events_information (
                                         id int PRIMARY KEY,
                                         event_id_ra varchar(500),
                                         country_id_ra varchar(500),
                                         event_name varchar(500),
                                         event_date varchar(500),
                                         club_id_ra varchar(500),
                                         event_follower varchar(500),
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
    for i in range(len(df)):
        sql = "INSERT INTO artists_information (id ,\
                            id_artist_ra,\
                            artist_name,\
                            artist_origin,\
                            artist_social_media,\
                            artist_nickname,\
                            artist_follower,\
                            artist_description,\
                            id_artist_collab,\
                            artist_famous_location,\
                            id_artist_most_played_club) \
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

        val = (i, unidecode(str(df["id"][i])),
               unidecode(str(df["Name"][i])),
               unidecode(str(df["Origin"][i])),
               unidecode(str(df["Online_account"][i])),
               unidecode(str(df["aka"][i])),
               unidecode(str(df["Followers"][i])),
               unidecode(str(df["Description"][i])),
               unidecode(str(df["Collaborations"][i])),
               unidecode(str(df["Famous_location"][i])),
               unidecode(str(df["Famous_clubs"][i])))

        cur.execute(sql, val)
    mydb.commit()
    cur.close()


def insert_label(df, db_filename):
    mydb = mysql.connector.connect(host="localhost", user="resident_advisor", db=db_filename, passwd="bicep",
                                   auth_plugin='mysql_native_password')
    cur = mydb.cursor()
    for i in range(len(df)):
        sql = '''INSERT INTO label_information (id ,\
                                  id_label_ra ,\
                                  label_name ,\
                                  label_creation ,\
                                  label_country ,\
                                  label_social_media ,\
                                  label_follower ,\
                                  label_description ,\
                                  label_artist) \
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

        val = (i, unidecode(str(df["id"][i])),
               unidecode(str(df["Name"][i])),
               unidecode(str(df["Creation"][i])),
               unidecode(str(df["Country"][i])),
               unidecode(str(df["Online_account"][i])),
               unidecode(str(df["Followers"][i])),
               unidecode(str(df["Description"][i])),
               unidecode(str(df["ids_artists"][i])))
        cur.execute(sql, val)
    mydb.commit()
    cur.close()


def insert_clubs(df, db_filename):
    mydb = mysql.connector.connect(host="localhost", user="resident_advisor", db=db_filename, passwd="bicep",
                                   auth_plugin='mysql_native_password')
    cur = mydb.cursor()
    for i in range(len(df)):
        sql = '''INSERT INTO clubs_information (id ,\
                                  id_country_ra ,\
                                  club_id_ra  ,\
                                  club_name ,\
                                  club_location ,\
                                  club_follower ,\
                                  club_phone ,\
                                  club_capacity ,\
                                  club_contact )\
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

        val = (i,
               unidecode(str(df["Club_Country"][i])),
               unidecode(str(df["Club_ID"][i])),
               unidecode(str(df["Club_Name"][i])),
               unidecode(str(df["Club_Location"][i])),
               unidecode(str(df["Club_Follower"][i])),
               unidecode(str(df["Club_Phone"][i])),
               unidecode(str(df["Club_Capacity"][i])),
               unidecode(str(df["Club_Contact"][i])))
        cur.execute(sql, val)
    mydb.commit()
    cur.close()


def insert_events(df, db_filename):
    mydb = mysql.connector.connect(host="localhost", user="resident_advisor", db=db_filename, passwd="bicep",
                                   auth_plugin='mysql_native_password')
    cur = mydb.cursor()
    for i in range(len(df)):
        sql = '''INSERT INTO events_information (id ,\
                                      event_id_ra ,\
                                      country_id_ra ,\
                                      event_name ,\
                                      event_date ,\
                                      club_id_ra ,\
                                      event_follower ,\
                                      event_lineup ,\
                                      event_artist) \
                                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)'''

        val = (i, unidecode(str(df["Event_ID"][i])),
               unidecode(str(df["Country_ID"][i])),
               unidecode(str(df["Event_Name"][i])),
               unidecode(str(df["Event_Date"][i])),
               unidecode(str(df["Event_Location"][i])),
               unidecode(str(df["Event_Follower"][i])),
               unidecode(str(df["Event_Lineup"][i])),
               unidecode(str(df["Event_Artists"][i])))
        cur.execute(sql, val)
    mydb.commit()
    cur.close()
