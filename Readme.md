# Quora ads analytics

API reference https://www.quora.com/ads/api9169a6d6e9b42452d500a61717d87d15d5fa49ec5b53030741178130#

## Authentification

The "auth.py" has a function(refresh_token) that will fetch the required 
credentials from google secret manager and also calls the
endpoint to refresh the token.


## Ads api

1. fetching all ads in account
2. fetching data for each ad

```
python quora.py
```
this will fetch data from quora api and save it in results file