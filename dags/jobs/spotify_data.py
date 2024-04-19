import os

import pandas as pd
from dotenv import load_dotenv
import requests
import time


load_dotenv()

client_id = os.environ.get('SPOTIFY_CLIENT_ID')
client_secret = os.environ.get('SPOTIFY_CLIENT_SECRET')




import json
genres = ['Gospel', 'R&B', 'pop', 'rock', 'hip-hop']

# Get an access token from Spotify
def get_access_token(client_id, client_secret):
    url = 'https://accounts.spotify.com/api/token'
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = {'grant_type': 'client_credentials'}
    response = requests.post(url, auth=(client_id, client_secret), headers=headers, data=data)
    if response.status_code == 200:
        token_data = response.json()
        access_token = token_data.get('access_token')
        return access_token
    else:
        print(f"Error getting access token: {response.status_code} - {response.text}")
        return None


# Search for tracks of a specific genre and return artists
def search_artists_by_genre(access_token, genre_list):
    url = 'https://api.spotify.com/v1/search'
    headers = {'Authorization': f'Bearer {access_token}'}
    all_artists = []
    for genre in genre_list:
        params = {'q': f'genre:{genre}', 'type': 'artist', 'limit': 50}
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            artists = response.json().get('artists', {}).get('items', [])
            all_artists.extend(artists)
            return all_artists
        else:
            print(f"Error searching for artists: {response.status_code} - {response.text}")
            return None


def search_albums_by_artist_id(access_token, artist_id):
    url = 'https://api.spotify.com/v1/search'
    headers = {'Authorization': f'Bearer {access_token}'}
    params = {'q': f'artist:{artist_id}', 'type': 'album', 'limit': 5}
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        albums = response.json().get('albums', {}).get('items', [])
        return albums
    else:
        print(f"Error getting albums: {response.status_code} - {response.text}")
        return None


def search_albums_by_artist_name(access_token, artist_name):
    url = 'https://api.spotify.com/v1/search'
    headers = {'Authorization': f'Bearer {access_token}'}
    params = {'q': f'artist:{artist_name}', 'type': 'album', 'limit': 5}
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        albums = response.json().get('albums', {}).get('items', [])
        return albums
    else:
        print(f"Error getting albums: {response.status_code} - {response.text}")
        return None


def search_tracks_by_album_name(access_token, album_name):
    url = 'https://api.spotify.com/v1/search'
    headers = {'Authorization': f'Bearer {access_token}'}
    params = {'q': f'album:{album_name}', 'type': 'track', 'limit': 5}
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        tracks = response.json().get('tracks', {}).get('items', [])
        return tracks
    else:
        print(f"Error searching for tracks: {response.status_code} - {response.text}")
        return None


def search_playlists_by_album_name(access_token, album_name):
    url = 'https://api.spotify.com/v1/search'
    headers = {'Authorization': f'Bearer {access_token}'}
    params = {'q': f'album:{album_name}', 'type': 'playlist', 'limit': 5}
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        playlists = response.json().get('playlists', {}).get('items', [])
        return playlists
    else:
        print(f"Error searching for playlists: {response.status_code} - {response.text}")
        return None


def generate_spotify_data(genres):
    access_token = get_access_token(client_id, client_secret)
    artists = search_artists_by_genre(access_token=access_token, genre_list=genres)
    for artist in artists:
        all_albums = []
        time.sleep(2)
        albums = search_albums_by_artist_name(access_token=access_token, artist_name=artist['name'])
        for album in albums:
            time.sleep(2)
            tracks = search_tracks_by_album_name(access_token=access_token, album_name=album["name"])
            time.sleep(2)
            playlists = search_playlists_by_album_name(access_token=access_token, album_name=album["name"])
            album_obj = {
                "name": album["name"],
                "id": album["id"],
                "tracks": tracks,
                "playlists": playlists,
            }
            all_albums.append(album_obj)
        artist_obj = {
            "type": artist["type"],
            "id": artist["id"],
            "name": artist["name"],
            "popularity": artist["popularity"],
            "genres": artist["genres"],
            "followers": artist["followers"],
            "albums": all_albums,
        }
        input_data = pd.DataFrame([artist_obj])
        output_data = input_data.apply(transform_data, axis=1)
        with open('data/' + artist_obj["id"] + '_data.json', 'w') as f:
            json.dump(output_data.to_dict(orient='records')[0], f)


def transform_data(row):
    transformed_albums = []
    for album in row['albums']:
        transformed_tracks = []
        if 'tracks' in album and isinstance(album['tracks'], list):
            for track in album['tracks']:
                transformed_track = {
                    'album': {
                        'artists': [{
                            'id': row['id'],
                            'name': row['name']
                        }],
                        'available_markets': track.get('available_markets', []),
                        'id': album.get('id', ''),
                        'name': album.get('name', ''),
                        'release_date': album.get('release_date', ''),
                        'release_date_precision': album.get('release_date_precision', ''),
                        'total_tracks': album.get('total_tracks', 0)
                    },
                    'artists': [{
                        'id': row['id'],
                        'name': row['name']
                    }] + track.get('artists', []),
                    'available_markets': track.get('available_markets', []),
                    'disc_number': track.get('disc_number', 0),
                    'duration_ms': track.get('duration_ms', 0),
                    'explicit': track.get('explicit', False),
                    'id': track.get('id', ''),
                    'is_local': track.get('is_local', False),
                    'name': track.get('name', ''),
                    'popularity': track.get('popularity', 0),
                    'preview_url': track.get('preview_url', ''),
                    'track_number': track.get('track_number', 0)
                }
                transformed_tracks.append(transformed_track)

        transformed_album = {
            'name': album.get('name', ''),
            'id': album.get('id', ''),
            'tracks': transformed_tracks
        }
        transformed_albums.append(transformed_album)

    transformed_data = {
        'type': row.get('type', ''),
        'id': row.get('id', ''),
        'name': row.get('name', ''),
        'popularity': row.get('popularity', 0),
        'genres': row.get('genres', []),
        'followers': row['followers']['total'] if 'followers' in row and 'total' in row['followers'] else 0,
        'albums': transformed_albums
    }
