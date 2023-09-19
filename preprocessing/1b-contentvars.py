import gc
import pandas as pd
import pickle
from top2vec import Top2Vec
import numpy as np
import tensorflow as tf
import tensorflow_hub as hub
import tensorflow_text
from pandarallel import pandarallel

pandarallel.initialize(progress_bar=False,use_memory_fs=False)

def concept_sim(doc_vec, concept_vec):
    return np.inner(doc_vec, concept_vec)

def save_func(df, fn, sp):
    df.to_csv(sp+fn+'.csv')
    return

def samp_div_var(df, div_v):
    #sample 100 project descriptions in 90th percentile of diversity variable
    df['pct'] = df[div_v].rank(pct=True)
    top_essays = df[df['pct']>0.9].sample(n=100)
    return top_essays

projects_path = '/scratch/gpfs/el1847/dec2022data/processed_data/filtered_essay_texts.csv'
df = pd.read_csv(projects_path)
sample = df.sample(frac=0.5, random_state=123)
rest=df.drop(sample.index)

s_docs = sample.project_essay_text.tolist()
s_ids = sample.project_id.tolist()
r_docs = rest.project_essay_text.tolist()
r_ids = rest.project_id.tolist()
del(sample)
del(rest)
gc.collect()

t2v = Top2Vec(s_docs,
              embedding_model='universal-sentence-encoder',
              document_ids=s_ids,
              workers=8,
              embedding_model_path='/scratch/gpfs/el1847/universal-sentence-encoder_4',
              keep_documents=True,
              split_documents = True,
              chunk_length = 128,
              umap_args={'low_memory':True})
t2v.save("/scratch/gpfs/el1847/dec2022data/processed_data/t2vec_essay")
t2v.add_documents(r_docs, doc_ids=r_ids)
t2v.save("/scratch/gpfs/el1847/dec2022data/processed_data/t2vec_essay")

t2v = Top2Vec.load("/scratch/gpfs/el1847/dec2022data/processed_data/t2vec_essay")
id_vecdf = pd.DataFrame(zip(t2v.document_ids, t2v.document_vectors),
                        columns=['project_id', 'vector'])

categories = {"diversity" : ["diversity", "inclusivity", "multicultural", "multiethnic", "intercultural"],
              "race" : ["race", "racial", "ethnic", "multiracial", "asian", "multiethnic", "hispanic", "latino", "latina", "latinx", "biracial", "caucasian", "white", "black", "native", "anglo", "African American", "indigenous"],
              "justice" : ["justice", "equity", "equality", "fair", "unjust", "injustice", "inequity", "unfair", "rights"],
             "coexist" : ["coexist", "harmonious", "togetherness", "unify", "unifies", "commonality", "unity", "unite",
              "uniting", "acceptance", "tolerance", "cohesiveness"],
            'represent' : ["representation", "look like them", "see themselves"],
              "lgbt" : ["lgbt", "gay", "gsa", "queer", "sexuality", "bi", "lesbian", "transgender", "trans"],
              "disability" : ['disability', 'handicap', 'disabled', 'abled', 'impairment', 'impediment',
                'crippled', 'disabilites','iep', 'deafness', 'afflicted', 'debilitating', 'disorder',
                'dyscalculia', 'cochlear', 'wheelchairs','bifida', 'dysfunction', 'disadvantaged',
                'syndromes', 'dystrophy', 'neurotypical', 'hardship', 'deficient', 'assistive',
                 'stigmatized', 'disorders', 'dyslexia', 'inoperable', 'palsy','dyslexic', 'diagnosed', 
                  'dysgraphia', 'asperger', 'prosthetics', 'chronically', 'spina', 'downs',  'deformity','deaf', 
                  'apraxia', 'autism', 'mobility', 'overcome', 'epilepsy', 'accommodating'],
            "tech":['ipads','robotics','coding','technology',
                      'chromebooks','elearning','computer'],
              "sport":['gymnasium','athletics','sports','pe']}

concept_vectors = dict()
for cat in categories.keys():
    keywords = [t2v._embed_query(word) for word in categories[cat]]
    combined = t2v._get_combined_vec(keywords, [])
    concept_vectors[cat] = combined

for cat, vect in concept_vectors.items():
    id_vecdf[cat] = id_vecdf['vector'].parallel_apply(concept_sim, args=(vect,))

for cat in categories.keys():
    save_func(samp_div_var(id_vecdf, cat), fn=f"top_{cat}.csv", sp='/scratch/gpfs/el1847/dec2022data/processed_data/')

id_vecdf.to_pickle('/scratch/gpfs/el1847/dec2022data/processed_data/content_vars.pkl')

