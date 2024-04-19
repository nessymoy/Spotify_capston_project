# CAPSTONE PROJECT Spotify 
### Enhancing Spotify's Music Analytics Platform
## Overview
### This project implements an ETL(Extract, Transform, Load) pipeline for retrieving data from the Spotify API, transforming it, and loading it into AWS S3 and finally to Amazon Redshift.
## Architecture
![Image 02-03-2024 at 11 39](https://github.com/nessymoy/Spotify_capston_project/assets/136928658/b0833c05-f2d8-414b-819b-621e5f495c2d)

## The Stages of the project.
  1. INTEGRATION: Spotify API obtain detailed information about artists, tracks, albums, user interactions and playlists.
  2. ETL PROCESS extract data from Spotify using API key.
  3. Transform: structure and format raw data to fit predefined schema.
  4. Load: store transformed data into the PostgresSQL data warehouse Data should be constant and accurate
5. ETL automation: schedule the ETL process to run daily at 10pm.
6. Load data from AWS S3 to Amazon Redshift.

## Entity relationship Diagram.(ERD)
<img width="525" alt="Screenshot 2024-03-16 at 20 11 11" src="https://github.com/nessymoy/Spotify_capston_project/assets/136928658/8d20344e-7f86-4099-bf82-ba20efe00c6b">

## Results
1. Spotify wont allow to pull data mutliple time becuase of their securtiy reasons.
2. The number of calls made to spotify was huge therefore it took longer to read and collect the data.
<img width="1418" alt="Screenshot 2024-03-07 at 18 52 24" src="https://github.com/nessymoy/Spotify_capston_project/assets/136928658/bcb37cd0-8c1f-46bc-b135-78e35d505afc">
3. From the data collected from spotify, I was able to load the data to AWS and create tables as shown below.
   
![Image 19-04-2024 at 20 13](https://github.com/nessymoy/Spotify_capston_project/assets/136928658/f0f34251-4fb3-4655-8d6f-517b647a4238)
