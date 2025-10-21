"""
Twitter2Hive-Copy1
"""

#!/usr/bin/env python
# coding: utf-8

# In[2]:


import json
import tempfile
import requests
import pathlib
from datetime import datetime as dt
from uuid import uuid4
from requests_oauthlib import OAuth1Session


# In[3]:


consumer_key = '7GKwhJwazdDrgaXSzZvMrFGtT'
consumer_secret_key = 'ceLNls49QUtprrLCTDsjaXRLQJ4hGhKcIQKf4p7sfiXVcljZJM'

access_token = '551693427-9YxBcOd49wE6nKhNCeVX0RKURlc0rSPEMov9FoPR'
access_token_secret = 'UtKnwyytjObFs3RRXhVrIwruljYUB6xbCkzJf4nbzMVZf'

OUT_PATH = "/home/twitter22"
QUERY = "bolivia"
STOP_AFTER = 50


# In[4]:


pathlib.Path(OUT_PATH).mkdir(parents=True, exist_ok=True)
query_data = {
    "track": f"#{QUERY}".replace("#", "").lower(),
    "language":"es",
}


# In[5]:


twitter =  OAuth1Session(
    client_key=consumer_key,
    client_secret=consumer_secret_key,
    resource_owner_key=access_token,
    resource_owner_secret=access_token_secret,
)


# In[6]:


url = "https://stream.twitter.com/1.1/statuses/filter.json"
query_url = f"{url}?{'&'.join([f'{k}={v}' for k,v in query_data.items()])}"


# In[11]:


print(f"STREAMING {STOP_AFTER} TWEETS")
with twitter.get(query_url, stream=True) as response:
    for i, raw_tweet in enumerate(response.iter_lines()):
        if i == STOP_AFTER:
            break
        try:
            tweet = json.loads(raw_tweet)
            print(f"{i+1}/{STOP_AFTER}: {tweet['user']['screen_name']},{tweet['user']['location']} @ {tweet['created_at']}: {tweet['text']}\n")

        except (json.JSONDecodeError, KeyError) as err:
            print(f"{i+1}/{STOP_AFTER}: ERROR===>0of, encountered a mangled line of data here..\n")
            continue 

        with pathlib.Path(OUT_PATH) / f"{dt.now().timestamp()}_{uuid4()}.json" as F:
            F.write_bytes(raw_tweet)


# In[ ]:




