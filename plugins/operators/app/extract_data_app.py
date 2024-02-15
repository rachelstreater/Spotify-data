import time
import spotipy
import pandas as pd
from spotipy.oauth2 import SpotifyOAuth
from flask import Flask, request, session, redirect, url_for, make_response

# initialize Flask app
app = Flask(__name__)

# set the name of the session cookie
app.config['SESSION_COOKIE_NAME'] = 'Spotify Cookie'

# set a random secret key to sign the cookie
app.secret_key = 'vef84rf4edef2dcebwsnsd'

# set the key for the token info in the session dictionary
TOKEN_INFO = 'token_info'

# route to handle logging in
@app.route('/')
def login():
    # create a SpotifyOAuth instance and get the authorization URL
    auth_url = create_spotify_oauth().get_authorize_url()
    # redirect the user to the authorization URL
    return redirect(auth_url)

# route to handle the redirect URI after authorization
@app.route('/redirect')
def redirect_page():
    # clear the session
    session.clear()
    # get the authorization code from the request parameters
    code = request.args.get('code')
    # exchange the authorization code for an access token and refresh token
    token_info = create_spotify_oauth().get_access_token(code)
    # save the token info in the session
    session[TOKEN_INFO] = token_info
    # redirect the user to the save_top50 route
    return redirect(url_for('save_top50',_external=True))

# route to save the songs to a playlist
@app.route('/saveTop50')
def save_top50():
    try: 
        # get the token info from the session
        token_info = get_token()
    except:
        # if the token info is not found, redirect the user to the login route
        print('User not logged in')
        return redirect("/")

    # create a Spotipy instance with the access token
    sp = spotipy.Spotify(auth=token_info['access_token'])


    # set up
    playlist_id = '37i9dQZEVXbLRQDuF5jeBp'
    playlist_features_list = ['artist', 'album', 'track_name', 'track_id']
    
    tracks_df = pd.DataFrame(columns = playlist_features_list)
    
    # get the tracks from the playlist
    playlist = sp.playlist_tracks(playlist_id)['items']
    
   # Extract track information using list comprehension
    track_list = [
        {
            "artist": track["track"]["album"]["artists"][0]["name"],
            "album": track["track"]["album"]["name"],
            "track_name": track["track"]["name"],
            "track_id": track["track"]["id"]
        }
        for track in playlist
    ]

    # Create a DataFrame from the list of dictionaries
    df = pd.DataFrame(track_list)
    timestamp = time.time()
    df['timestamp'] = timestamp
    
    # convert the df to json
    json_data = df.to_json(orient='records')
    
    # Send JSON response with DataFrame data
    response = make_response(json_data)
    response.headers['Content-Type'] = 'application/json'
    
    # # write to temp file
    # TEMP_FILE_PATH = f'/tmp/spotify_data{timestamp}.csv'
    # df.to_csv(TEMP_FILE_PATH, encoding='utf-8')
    
    # return a success message
    return response

# function to get the token info from the session
def get_token():
    token_info = session.get(TOKEN_INFO, None)
    if not token_info:
        # if the token info is not found, redirect the user to the login route
        redirect(url_for('login', _external=False))
    
    # check if the token is expired and refresh it if necessary
    now = int(time.time())

    is_expired = token_info['expires_at'] - now < 60
    if(is_expired):
        spotify_oauth = create_spotify_oauth()
        token_info = spotify_oauth.refresh_access_token(token_info['refresh_token'])

    return token_info

def create_spotify_oauth():
    return SpotifyOAuth(
        client_id='7ce3d8a89a2f4c93b8b137bcfe460c9e',
        client_secret='9f19398d50934b9dba9ccba4613eb4ca',
        redirect_uri = url_for('redirect_page', _external=True),
        scope='user-library-read playlist-modify-public playlist-modify-private'
    )
    

if __name__ == '__main__':
    app.run(debug=True)

# TODO: move the client_id and client_secret to an env 