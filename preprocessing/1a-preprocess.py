import pandas as pd
from pandarallel import pandarallel

pandarallel.initialize(progress_bar=False,use_memory_fs=False)
def vader_func(text):
    from nltk import tokenize
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    paragraphSentiments = 0.0
    sentence_list = tokenize.sent_tokenize(text)
    vader = SentimentIntensityAnalyzer()
    for sentence in sentence_list:
        vs = vader.polarity_scores(sentence)
        paragraphSentiments += vs["compound"]

    try: 
        return paragraphSentiments / len(sentence_list)
    except: 
        return None

df=pd.read_csv('/scratch/gpfs/el1847/dec2022data/projects.csv')
donations = pd.read_csv('/scratch/gpfs/el1847/dec2022data/donations.csv')

#rename columns
col_names = [s.replace(" ", "_") for s in df.columns]
df.columns = col_names

#year and month
df['project_posted_date'] = pd.to_datetime(df['project_posted_date'])
df['year'] = df['project_posted_date'].dt.year
df['month'] = df['project_posted_date'].dt.month

#donations variables
d_colnames = [s.replace(" ", "_") for s in donations.columns]
donations.columns = d_colnames
donations['matched'] = donations['donation_payment_was_matched_(yes_/_no)']=='Yes'
donations['referred'] = donations['donation_is_teacher_referred_(yes_/_no)'] == 'Yes'
donations_df = donations.groupby('donation_project_id').agg({'referred': 'mean',
'matched':'max'}).reset_index()
df = df.merge(donations_df, how='left', left_on='project_id', right_on='donation_project_id')

#filter prior 2007 and reallocated and live
df = df[df['year'] >= 2007]
df = df[(df['project_status_as_of_12/8/2022'] != 'reallocated') & (df['project_status_as_of_12/8/2022'] != 'live')] 
#filter funded but no donations data
df = df[((df['project_status_as_of_12/8/2022']=='completed') & (df['matched'].isnull()))==False]
#filter no essay
df = df[df['project_essay_text'].notnull()]

#word count
df['essay_wcount'] = df['project_essay_text'].parallel_apply(lambda x : len(x.split()))

#vader
df['vader'] = df['project_essay_text'].parallel_apply(vader_func)

#success
df['success'] = df['project_status_as_of_12/8/2022']=='completed'
df['success'] = df['success'].astype('int')

df.to_pickle('/scratch/gpfs/el1847/dec2022data/processed_data/preprocessed.pkl')

#export essay_texts for R 
df[['project_id','year','month','project_essay_text']].to_csv('/scratch/gpfs/el1847/dec2022data/processed_data/filtered_essay_texts.csv')