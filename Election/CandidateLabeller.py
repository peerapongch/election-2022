import pandas as pd
from tqdm import tqdm

# identifying posts that talk about certain candidates
def match_by_and(x,y):
  ylist = y.split('+')
  try:
    for i in ylist:
      if x.find(i)!=-1:
        x = x.replace(i,'')
      else:
        return False
    return True
  except:
    return False

def find_keywords(x,c,candidates):
  keyword_match = [x.find(y)!=-1 for y in candidates[c]['keywords']]
  if any(keyword_match):
    return candidates[c]['keywords'][keyword_match.index(True)]
  else:
    return ''

def find_and(x,c,candidates):
  and_match = [match_by_and(x,y) for y in candidates[c]['and_keywords']]
  if any(and_match):
    return candidates[c]['and_keywords'][and_match.index(True)]
  else:
    return ''

def label_candidates(data,candidates):
  df = pd.DataFrame(data)

  for c in candidates.keys():
    print('-'*30)
    print(f'Computing for {candidates[c]["name"]}')

    df[f'can{c}_keywords'] = [find_keywords(x,c,candidates) for x in tqdm(df['full_text'])]
    df[f'can{c}_and'] = [find_and(x,c,candidates) for x in tqdm(df['full_text'])]

    df[f'can{c}'] = [1 if max(len(x),len(y))>0 else 0 for x,y in zip(df[f'can{c}_keywords'],df[f'can{c}_and'])]

  n = df.shape[0]

  fdf = df[df[[f'can{x}' for x in candidates.keys()]].sum(axis=1)>0]

  n_related = fdf.shape[0]

  print(f'Looking at {n_related} of the total {n} records ({round(n_related/n*100,2)}%)')

  return df