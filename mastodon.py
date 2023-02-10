# This script extracts posts (officially called 'statuses' or 'toots') from the Mastodon API and loads them to Bigquery

# import packages
import requests
import json
from pprint import pprint
from bs4 import BeautifulSoup

# create function to get statuses
def get_posts():

    # Make GET request to the API endpoint
    auth = {'Authorization': 'Bearer ExAh3LOJC0T9R2ivYQYIBdVeaNg3tQkkbS5s30g-nyU'}
    url = 'https://data-folks.masto.host//api/v1/timelines/tag/:hiring' # API endpoint
    params = {'all':['data'], 'limit': 2}

    response = requests.get(url, data=params, headers=auth)

    # Check the response status code
    if response.status_code == 200:
        
        # Convert response to JSON
        data = response.json()

        #Extract statuses from the JSON
        posts = []

        for idx, item in enumerate(data):
            id = data[idx]['id']
            url = data[idx]['url']
            created_at = data[idx]['created_at']
            content_raw = data[idx]['content']
            acct = data[idx]['account']['acct']

            # Remove HTML from text
            soup = BeautifulSoup(content_raw,'html.parser')
            content = soup.get_text()

            post = {
            'id': id,
            'created_at': created_at,
            'content': content,
            'url': url,
            'acct': acct
            }

            posts.append(post)

        return posts

    else:
        print("Request failed with status code:", response.status_code)

# write result to a JSONL file
filename = 'mastodon.jsonl'
   
with open(filename, "w") as f:
    for line in get_posts():
        f.write(json.dumps(line) + "\n") 

print("File updated: {}".format(filename))







