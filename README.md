# CAPSTONE PROJECT Spotify 
### Enhancing Spotify's Music Analytics Platform
## Overview


### This project implements an ETL(Extract, Transform, Load) pipeline for retrieving data from the Spotify API, transforming it, and loading it into AWS S3 and finally to Amazon Redshift.

Data was extracted out of the Spotify API using

## Architecture
![Image 02-03-2024 at 11 39](https://github.com/nessymoy/Spotify_capston_project/assets/136928658/b0833c05-f2d8-414b-819b-621e5f495c2d)



## The Stages of the project.
  1. INTEGRATION: Spotify API obtain detailed information about artists, tracks, albums, user interactions and playlists.
  2. ETL PROCESS extract data from Spotify using API key.
  3. Transform: structure and format raw data to fit predefined schema.
  4. Load: store transformed data into the PostgresSQL data warehouse Data should be constant and accurate
5. ETL automation: schedule the ETL process to run daily at 10pm.
6. Load data from AWS S3 to Amazon Redshift.


![Image 06-03-2024 at 19 31](https://github.com/nessymoy/Spotify_capston_project/assets/136928658/02556c54-2953-4d34-89d4-4bcb85b6c9f0)


## Entity relationship Diagram.(ERD)
![Image 02-03-2024 at 11 47](https://github.com/nessymoy/Spotify_capston_project/assets/136928658/95af28d7-64f4-4a9d-94fc-e116813b05fe)




