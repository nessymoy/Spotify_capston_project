import spotipy
from pandas import json_normalize
from spotipy.oauth2 import SpotifyClientCredentials
import os
from dotenv import load_dotenv

# %%
import pandas as pd

load_dotenv()
client_id = os.environ.get('SPOTIFY_CLIENT_ID')
client_secret = os.environ.get('SPOTIFY_CLIENT_SECRET')

client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)


def get_artists_by_genre(genre_list):
    all_artists = []
    for genre in genre_list:
        # Search for artists by genre
        results = sp.search(q=f'genre:{genre}', type='artist', limit=50)

        # Extract and print artist information
        artists = results['artists']['items']
        all_artists.extend(artists)
    return artists


def get_albums_by_artist(artist_id):
    albums = sp.artist_albums(artist_id, album_type='album', limit=10)
    return albums


def get_album_tracks(album_id):
    # Get the album's tracks
    tracks = sp.album_tracks(album_id, limit=10)
    return tracks['items']


def get_playlists_for_album(album):
    # Search for playlists containing the album
    query = f"album:{album['name']}"
    playlists = sp.search(q=query, type='playlist', limit=10)
    return playlists['playlists']['items']


def generate_spotify_data(genres):
    tracks = []
    playlists = []
    artists = get_artists_by_genre(genre_list=genres)
    for artist in artists:
        artist_id = artist['id']

        albums = get_albums_by_artist(artist_id=artist_id)
    for album in albums["items"]:
        my_tracks = get_album_tracks(album_id=album["id"])
        tracks.extend(my_tracks)

        my_playlist = get_playlists_for_album(album=album)
        playlists.extend(my_playlist)

    data = {
        "artists": artists,
        "albums": albums,
        "tracks": tracks,
        "playlists": playlists
    }
    print("##########==> ARTISTS")

    # Flatten the nested JSON structure using json_normalize
    artists_df = pd.json_normalize(data['artists'])

    # Select the desired columns
    columns_to_keep = ['followers.total', 'genres', 'id', 'name', 'popularity', 'type']
    df_restructured = artists_df[columns_to_keep]

    # Rename the columns
    df_restructured.columns = ['followers', 'genres', 'id', 'name', 'popularity', 'type']

    # Convert the 'followers' column to a dictionary and explicitly cast 'total' to int
    df_restructured['followers'] = df_restructured['followers'].apply(lambda x: {'total': int(x)})

    # Convert to a dictionary
    # artists_result_dict = df_restructured.to_dict(orient='records')

    # Print the result
    # print(artists_result_dict)
    artists_df.to_csv('artists.csv', index=False)
    print("##########==> ALBUMS")
    # Flatten the 'artists' column with a prefix
    albums_df = pd.json_normalize(data["albums"]["items"])

    # Remove 'album_group' and 'album_type' fields
    albums_df = albums_df.drop(['album_group', 'album_type'], axis=1, errors='ignore')

    # Filter columns
    albums_df = albums_df[
        ['artists', 'id', 'name', 'release_date', 'release_date_precision', 'total_tracks', 'available_markets']]

    # Modify artists column to match the desired output
    albums_df['artists'] = albums_df['artists'].apply(
        lambda x: [{'id': x[0]['id'], 'name': x[0]['name'], 'type': x[0]['type']}])

    # Display the resulting DataFrame
    # print(albums_df.to_dict(orient='records'))
    albums_df.to_csv('albums.csv', index=False)
    print("##########==> TRACKS")
    tracks_df = pd.DataFrame(data["tracks"])

    # Restructure the DataFrame
    restructured_tracks = tracks_df[
        ['artists', 'available_markets', 'disc_number', 'duration_ms', 'explicit', 'id', 'is_local', 'name',
         'track_number']].copy()

    # Extract relevant information from nested dictionaries in the 'artists' column
    restructured_tracks['artists'] = restructured_tracks['artists'].apply(
        lambda x: [{'id': artist['id'], 'name': artist['name']} for artist in x])

    # Display the restructured data
    tracks_df.to_csv('tracks.csv', index=False)
    # print(restructured_tracks.to_dict(orient='records'))
    print("##########==> PLAYLISTS")

    # Create a DataFrame from the list of dictionaries
    playlists_df = pd.DataFrame(data["playlists"])

    # Restructure the DataFrame
    restructured_playlists = playlists_df[
        ['collaborative', 'description', 'id', 'name', 'owner', 'primary_color', 'public', 'tracks']].copy()

    # Extract relevant information from nested dictionaries in the 'owner' column
    restructured_playlists['owner'] = restructured_playlists['owner'].apply(
        lambda x: {'display_name': x['display_name'], 'id': x['id'], 'type': x['type']})

    # Display the restructured data
    # print(restructured_playlists.to_dict(orient='records'))
    playlists_df.to_csv('playlists.csv', index=False)
    return data
