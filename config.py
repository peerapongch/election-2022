# DB
## connection credentials/
## read from/
## write to/

# run scope
## date range
## candidate_id list

import os
from datetime import datetime
from dateutil.relativedelta import relativedelta

# CONNECTIVITY SETTINGS
BASTION_HOST = "178.128.31.43"
BASTION_USER = "root"
BASTION_PASS = "POC_2020AION"

DB_HOST = '10.130.143.184'
DB_PORT = 27017
DB_USER = 'root'
DB_PASS = 'POC_2020AION'
DB_AUTH_SOURCE = 'admin'
DB_AUTH_MECH = 'SCRAM-SHA-1'
DB_NAME = 'aion'

LOCAL_BIND_HOST = '127.0.0.1'
LOCAL_BIND_PORT = 27017

# CONNECTION RULES
DATA_COLLECTION = 'user_posts'
KEYWORDS_COLLECTION = 'object'
REPORT_COLLETION = 'election_report'

# MAPPING SCOPES
DATE_FROM = datetime(2022,4,1)
DATE_TO = datetime(2022,5,5)
CANDIDATE_IDS = [1,4,6,7,8]
TOPICS_DIR = './data/topics/'


DATA_RULE = query = [
  {
    "$match": {
        "$and": [
                  {
                      'date': {
                          '$gte': DATE_FROM,
                          '$lt': DATE_TO
                          }
                  }
        ]
    },
  },
  { 
      "$project": {
          "full_text": 1, 
          "sentiment": 1,
          "date": 1,
          "likes_count": 1,
          "comments_count": 1,
          "retweets_count": 1,
          "engagement": 1,
          "follower": 1,
          "reply_to": 1,
          "account_name": 1,
          "cluster": 1,
          "people": 1
      }
  },

]

KEYWORDS_RULE = [
  {
      "$match": {
          'id': {
              '$in': [575,576,577,579,580,581,696,699]
          }
      },
  },
  {
      "$project": {
          "name": 1,
          "keywords": 1,
          "and_keywords": 1
      }
  }
]