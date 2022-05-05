# connect DB
# read in data from given scope
# read in mappings (topics, candidate_id)
# mapping candidates
# mapping topics
# output to a new collection

import pymongo
import pickle
import time
from sshtunnel import SSHTunnelForwarder
from tqdm import tqdm

from config import *
from Election.CandidateLabeller import label_candidates
from Election.TopicLabeller import make_topic_report

def connect_db():
  print('-'*30)
  print('Establishing Connection with Aion MongoDB')
  # define ssh tunnel
  success = False
  while not success:
    try:
      server = SSHTunnelForwarder(
          BASTION_HOST,
          ssh_username = BASTION_USER,
          ssh_password = BASTION_PASS,
          remote_bind_address=(DB_HOST, DB_PORT),
          local_bind_address=(LOCAL_BIND_HOST, LOCAL_BIND_PORT)
      )

      # tunnel and bind port
      server.start()

      # connection
      connection = pymongo.MongoClient(
          host = server.local_bind_host,
          port = server.local_bind_port,
          username = DB_USER,
          password = DB_PASS,
          authSource = DB_AUTH_SOURCE,
          authMechanism = DB_AUTH_MECH
      )
      db = connection[DB_NAME]

      success = True
      return db, connection, server

    except KeyboardInterrupt as e:
      raise e

    except Exception as e:
      print('connection to db failed: retrying')

def load_data(db,ingestation_rule,read_from_collection):

  success = False
  # for posts data use
  # while not success:
  #   try:
  #     db, connection, server = connect_db()
  #     success = True
  #   except Exception as e:
  #     print('error reading data')

  # db, connection, server = connect_db()

  posts = db[read_from_collection]\
  .aggregate(ingestation_rule)
  data = [x for x in tqdm(posts)]
  # connection.close()
  # server.stop()

  return data

def insert_data(db,data_list,insert_to_collection):
  # success = False
  # while not success:
  #   try:
  #     db, connection, server = connect_db()

  #   except Exception as e:
  #     print('data insert failed')

  # db, connection, server = connect_db()

  for x in tqdm(data_list):
    db[insert_to_collection].insert_one(x)
  
  # connection.close()
  # server.stop()

def get_candidates(db):

  keywords = load_data(db,KEYWORDS_RULE,KEYWORDS_COLLECTION)

  candidates = {}
  candidates[-1] = {}
  candidates[-1]['name'] = 'general เบอร์ 0'
  candidates[-1]['keywords'] = []
  candidates[-1]['and_keywords'] = []
  for i,x in enumerate(keywords):
    if x['name'] not in ['Hashtag','ปัญหา กทม.','วิสัยทัศน์']:
      candidates[i] = {}
      candidates[i]['name'] = x['name']
      candidates[i]['keywords'] = x['keywords'] 
      candidates[i]['and_keywords'] = x['and_keywords']
    else:
      candidates[-1]['keywords'] = candidates[-1]['keywords'] + x['keywords'] 
      candidates[-1]['and_keywords'] = candidates[-1]['and_keywords'] + x['and_keywords']

  candidates = {
      candidates[x]['name'].replace('**','').strip().split(' ')[-1].replace(')',''):candidates[x] for x in candidates
      }

  return candidates

def write_report(db,topic_report,report_collection=REPORT_COLLETION):
  print('-'*30)
  print('writing report')

  insert_data(db,topic_report,report_collection)

if __name__ == '__main__':
  
  db, connection, server = connect_db()

  # get post data and label candidates
  print('loading posts')
  data = load_data(db,DATA_RULE,DATA_COLLECTION)
  print('loading candidates')
  candidates = get_candidates(db)
  df = label_candidates(data,candidates)

  # match topics
  topic_report_list = make_topic_report(df,CANDIDATE_IDS,TOPICS_DIR)

  # write report
  write_report(db,topic_report_list)

  connection.close()
  server.stop()

  print('-'*30)
  print('done')