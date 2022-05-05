import pandas as pd
from tqdm import tqdm
import regex as re
import os
from datetime import datetime

def get_topics2(candidate_id,topics_dir):
  # clean topics
  # topics = pd.read_csv(f'../drive/MyDrive/Baikal/temp/election/topics_can{str(candidate_id)}.csv')
  topics_path = os.path.join(
    topics_dir,
    f'topics_can{str(candidate_id)}.csv'
  )
  topics = pd.read_csv(topics_path)

  print(f'fetched topic {topics_path}')

  # sum
  topics = topics.groupby(['topic','keyword']).sum().reset_index()
  # standardise
  topics[['sentiment_neg','sentiment_neu','sentiment_pos']] = topics[['sentiment_neg','sentiment_neu','sentiment_pos']].apply(lambda x: x/x.sum(),axis=1)

  topic_mapper = {}

  for topic in ['affiliation','campaign','competency','policy','public-sentiment','quality']:

    # topics.groupby(['keyword']).agg({'topic':pd.Series.nunique}).sort_values('topic',ascending=False)
    topic_mapper[topic] = topics[topics['topic']==topic].set_index('keyword').to_dict(orient='index')

  print(candidate_id)
  return topic_mapper

def generate_report2(data,candidate_id,topics_candidates):
  def clean(text):
    
    def remove_at_tag(text: str) -> str:
      return re.sub(r'@([a-zA-Z0-9_]+)', '', text)

    def remove_http(text: str) -> str:
      return re.sub(r'http://([a-zA-Z0-9_]+)', '', text)

    def remove_https(text: str) -> str:
      return re.sub(r'https://([a-zA-Z0-9_]+)', '', text)

    text = remove_at_tag(text)
    text = remove_http(text)
    text = remove_https(text)
    # text = text.replace('\n','').replace('“','').replace('”','')
    text = text.replace('\n','')

    return text

  def match_topic(row, topic_mapper={}):
    # print(row)
    for k in topic_mapper:
      if row['clean_full_text'].find(k)!=-1:
        return pd.Series([
                          k,
                          topic_mapper[k]['topic'],
                          topic_mapper[k]['sentiment_neg'],
                          topic_mapper[k]['sentiment_neu'],
                          topic_mapper[k]['sentiment_pos']
        ])
    return pd.Series(['NA','NA',0,0,0])

  ##### START HERE ######
  topic_mapper = topics_candidates[candidate_id]
  fdata = data[data[f'can{str(candidate_id)}']==1].reset_index(drop=True)
  fdata['clean_full_text'] = [clean(x) for x in fdata['full_text']]

  print(fdata.shape)  

  matched_data = []

  for topic in topic_mapper.keys():
    this_data = fdata.copy()
    this_data[['keyword_match','topic_match','topic_neg','topic_neu','topic_pos']] = this_data[['clean_full_text']].apply(match_topic,axis=1,topic_mapper=topic_mapper[topic])
    this_data = this_data[this_data['topic_match']==topic]

    matched_data.append(this_data)

  matched_data = pd.concat(matched_data)

  matched_data[['likes_count','comments_count','engagement','follower','retweets_count']] = matched_data[['likes_count','comments_count','engagement','follower','retweets_count']].fillna(0).astype('int32')
  # fdata[['likes_count','comments_count','engagement','follower']] = .astype('int32')
  # fdata.dtypes

  summ = matched_data.groupby(['topic_match','keyword_match']).agg(
      count = pd.NamedAgg('_id',pd.Series.nunique),
      total_likes = pd.NamedAgg('likes_count','sum'),
      total_comments = pd.NamedAgg('comments_count','sum'),
      total_engagement = pd.NamedAgg('engagement','sum'),
      total_followers = pd.NamedAgg('follower','sum'),
      unique_accounts = pd.NamedAgg('account_name',pd.Series.nunique),
      total_neg = pd.NamedAgg('topic_neg','sum'),
      total_neu = pd.NamedAgg('topic_neu','sum'),
      total_pos = pd.NamedAgg('topic_pos','sum')
  ).reset_index()

  likes = summ[summ['total_likes']>0]\
  .sort_values(['topic_match','total_likes'],ascending=False).groupby('topic_match').head(5).reset_index()
  likes = likes.groupby('topic_match')['keyword_match'].apply(list).reset_index().rename({'keyword_match':'top_likes'},axis=1)

  engage = summ[summ['total_engagement']>0]\
  .sort_values(['topic_match','total_engagement'],ascending=False).groupby('topic_match').head(5).reset_index()
  engage = engage.groupby('topic_match')['keyword_match'].apply(list).reset_index().rename({'keyword_match':'top_engagement'},axis=1)

  neg = summ[summ['total_neg']>0]\
  .sort_values(['topic_match','total_neg'],ascending=False).groupby('topic_match').head(5).reset_index()
  neg = neg.groupby('topic_match')['keyword_match'].apply(list).reset_index().rename({'keyword_match':'top_neg'},axis=1)

  pos = summ[summ['total_pos']>0]\
  .sort_values(['topic_match','total_pos'],ascending=False).groupby('topic_match').head(5).reset_index()
  pos = pos.groupby('topic_match')['keyword_match'].apply(list).reset_index().rename({'keyword_match':'top_pos'},axis=1)

  tops = likes\
  .merge(engage,how='outer',on='topic_match')\
  .merge(neg,how='outer',on='topic_match')\
  .merge(pos,how='outer',on='topic_match')

  report = summ.groupby('topic_match').sum().reset_index()\
  .merge(tops,on='topic_match',how='left')
  
  print(candidate_id)
  return report, summ

def make_topic_report(data,candidate_ids,topics_dir):
  # topics
  topics_candidates = {x:get_topics2(x,topics_dir) for x in candidate_ids}

  # make report
  summs = {}
  reports = {}
  for x in candidate_ids:
    report, summ = generate_report2(data,x,topics_candidates)
    reports[x] = report
    summs[x] = summ

  for x in candidate_ids:
    reports[x]['candidate_id'] = x

  master = pd.concat([
    reports[x] for x in candidate_ids
  ])

  # standardise score across candidates
  scores = ['total_engagement','total_likes','total_comments','total_followers','unique_accounts','total_neg','total_neu','total_pos']
  topics = ['NA','affiliation','campaign','competency','policy','public-sentiment','quality']

  for j in range(len(topics)):
    master.loc[master['topic_match']==topics[j],scores] = master.loc[master['topic_match']==topics[j],scores]/master.loc[master['topic_match']==topics[j],scores].sum()

  # this part is especially for inserting
  master['created_on'] = datetime.now()
  master_list = master.to_dict(orient='records')

  return master_list