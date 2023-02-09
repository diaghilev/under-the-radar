# Extract statuses from Mastodon
### WORK IN PROGRESS. Currently evaluating alternative data sources

import requests

auth = {'Authorization': 'Bearer ExAh3LOJC0T9R2ivYQYIBdVeaNg3tQkkbS5s30g-nyU'}
url = 'https://data-folks.masto.host/api/v2/search' # Mastodon API endpoint for search
params = {'q': '#hiring data'} # Set keywords for the search

# Make GET request to the API endpoint
response = requests.get(url, data=params, headers=auth)

# Check the response status code
if response.status_code == 200:
    # Get the JSON data from the response
    data = response.json()
    #print(data)

    # Print the statuses that contain the keywords
    for status in data["statuses"]:
        print(status["content"])
else:
    print("Request failed with status code:", response.status_code)






