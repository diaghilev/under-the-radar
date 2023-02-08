# Extract statuses (posts) from Mastodon
### WORK IN PROGRESS. json contains data but needs parsing.

import requests

url = 'https://data-folks.masto.host/api/v2/search'
auth = {'Authorization': 'Bearer ExAh3LOJC0T9R2ivYQYIBdVeaNg3tQkkbS5s30g-nyU'}
params = {'q': 'data #hiring'}

# API response
response = requests.get(url, data=params, headers=auth)

# Response as JSON
json = response.json()
print(response.json())






