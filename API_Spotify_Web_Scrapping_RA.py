import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd

client_id = "de505a8b5f8749ad87ff27185159fe58"
client_secret = "638facd40d2c40b5a44dd1ca2773882a"
client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)


def get_artist_id(artist_name):
    """Get the Spotify id of an artist if exists from the API
        :param artist_name : Name of the Artist
        :return: The Artist spotify id
        """
    result = sp.search(artist_name)
    if len(result['tracks']['items']) != 0:
        list_of_artists = [result['tracks']['items'][0]['artists'][i]['name'] for i in
                           range(len(result['tracks']['items'][0]['artists']))]
        if artist_name in list_of_artists:
            ind = list_of_artists.index(artist_name)
            artist_id = result['tracks']['items'][0]['artists'][ind]['id']
            return artist_id
        else:
            return 0
    else:
        return 0


def get_artist_spotify_info(artist_id):
    """Get all info on an artist
            :param artist_id : Spotify ID of the artist
            :return: Dictionary with all artist spotify info
            """
    if artist_id != 0:
        return sp.artist(artist_id)
    else:
        return None


def get_artist_genres(artist_infos):
    """Get artist genres
                :param artist_infos : Dictionary with all artist spotify info
                :return: List of Artist genre
                """
    if artist_infos is not None:
        return artist_infos['genres']
    else:
        return None


def get_artist_followers(artist_infos):
    """Get artist spotify followers
                :param artist_infos : Dictionary with all artist spotify info
                :return: Spotify followers
                """
    if artist_infos is not None:
        return artist_infos['followers']['total']
    else:
        return None


def get_artist_spotifyurl(artist_infos):
    """Get artist spotify url
                :param artist_infos : Dictionary with all artist spotify info
                :return: Spotify URL
                """
    if artist_infos is not None:
        return artist_infos['external_urls']['spotify']
    else:
        return None


def get_artist_thumbnail(artist_infos):
    """Get artist spotify images
                    :param artist_infos : Dictionary with all artist spotify info
                    :return: link to Spotify images
                    """
    if artist_infos is not None:
        if len(artist_infos['images']) != 0:
            return (artist_infos['images'][2]['url'])
        else:
            return None
    else:
        return None


def get_artist_albums(artist_id):
    """Get artist albums name
                    :param artist_id : Artist Spotify ID
                    :return: List of artist albums
                    """
    if artist_id != 0:
        sp_albums = sp.artist_albums(artist_id, album_type='album')
        album_names = []
        album_uris = []
        for i in range(len(sp_albums['items'])):
            album_names.append(sp_albums['items'][i]['name'])
            album_uris.append(sp_albums['items'][i]['uri'].split(":")[-1])
        return list(zip(album_uris, album_names))
    else:
        return None


def get_artist_song(artist_name):
    """Get artist spotify albums
                    :param artist_name : Artist Name
                    :return: Dataframe with all album information of an artist
                    """
    artist_songs = pd.DataFrame()
    artist_albums = get_artist_albums(get_artist_id(artist_name))
    if artist_albums is not None:
        for i in range(len(artist_albums)):
            album_info = sp.album_tracks(artist_albums[i][0])
            artist, album_name, track_number, track_name, track_duration, track_preview_url = [], [], [], [], [], []
            for j in range(len(album_info)):
                artist.append(artist_name)
                album_name.append(artist_albums[i][1])
                try:
                    track_number.append(album_info['items'][j]['track_number'])
                except:
                    track_number.append(1)
                try:
                    track_name.append(album_info['items'][j]['name'])
                except:
                    track_name.append("UNNAMED")
                try:
                    track_duration.append(album_info['items'][j]["duration_ms"])
                except:
                    track_duration.append(0)
                try:
                    track_preview_url.append(album_info['items'][j]['preview_url'])
                except:
                    track_preview_url.append("No Preview")
            artist_album_songs = pd.DataFrame(
                {'artist_name': artist, 'album_name': album_name, 'track_number': track_number,
                 'track_name': track_name, "track_duration(ms)": track_duration,
                 "track_preview_url": track_preview_url})
            artist_songs = artist_songs.append(artist_album_songs, ignore_index=True)
        return artist_songs
    else:
        return None