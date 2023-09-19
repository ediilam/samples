
import pandas as pd


df = pd.read_pickle("/scratch/gpfs/el1847/dec2022data/preprocessed.pkl")
fk = pd.read_csv('/scratch/gpfs/el1847/dec2022data/fk_readnb.csv')
content = pd.read_pickle('/scratch/gpfs/el1847/dec2022data/contentvars.pkl')


fk.columns = ['ind','project_id', 'project_essay_text_y','project_resource_category_y','fk_read','essay_sentcount']

df = df.merge(fk, how='left', on = 'project_id')
df = df.merge(content, how='left', on='project_id')

del fk
del content

df.columns = ['project_id', 'project_title', 'project_short_description',
       'project_need_statement', 'project_essay_text', 'project_grade_level',
       'project_primary_subject', 'project_primary_subject_subcategory',
       'project_secondary_subject_(optional)',
       'project_secondary_subject_subcategory_(optional)',
       'project_resource_category', 'project_cost_of_requested_resources',
       'project_vendor_shipping_charges', 'project_payment_processing_charges',
       'project_sales_tax', 'project_fulfillment_&_labor_materials_cost',
       'project_optional_support',
       'project_total_price_excluding_optional_support',
       'school_percentage_free_and_reduced_price_lunch_eligible',
       'project_students_reached', 'project_posted_date',
       'project_status_as_of_12/8/2022',
       'project_number_of_days_between_posted_and_completed',
       'project_thank_you_note', 'project_impact_letter',
       'project_posted_sequence_number_per_teacher', 'project_school_id',
       'project_state', 'project_zip', 'project_city', 'project_teacher_id',
       'year', 'month', 'donation_project_id', 'referred', 'matched',
       'essay_wcount', 'vader', 'ind', 'project_essay_text_y',
       'project_resource_category_y', 'fk_read', 'essay_sentcount', 'vector',
       'diversity', 'race', 'justice', 'coexist', 'represent', 'lgbt',
       'disability', 'tech', 'sport', 'success']


interest_vars = ['diversity', 'race', 'justice', 'coexist', 'represent', 'lgbt', 'disability', 'tech', 'sport']
regression_vars = ['project_total_price_excluding_optional_support',
            'project_posted_sequence_number_per_teacher',
            'referred',
            'year',
            'month',
            'project_students_reached',
            'project_grade_level',
            'project_resource_category',
            'matched',
            'project_state',
            'essay_wcount',
            'fk_read',
            'vader',
            'success']

#filter greater than 0 wordcount
df = df[df['essay_wcount']>0]
df = df[df.fk_read.notnull()]

#check nulls in donations 
df[regression_vars].isnull().sum(axis = 0)

#exclude test teacher ids
all_ids = set(df['project_teacher_id'])
exclude = pd.read_csv('/scratch/gpfs/el1847/dec2022data/exclude_teacherid_schoolid.csv')
exclude_ids = set(exclude['project teacher id'])
keep_ids = pd.Series(list(all_ids - exclude_ids)).to_frame(name='project_teacher_id')
df = df.merge(keep_ids, how='inner', on='project_teacher_id')

#add school names
school_names = pd.read_csv('/scratch/gpfs/el1847/dec2022data/schoolname_ids.csv')
school_names.columns = ['project_school_name', 'project_school_id']
df = df.merge(school_names, how='left', on = 'project_school_id')

#fill zeros
fill_values = {'referred':0,'matched':0}
df = df.fillna(fill_values)

#checks
#df[df.essay_wcount<20][['project_id','project_status_as_of_12/8/2022','project_essay_text','essay_wcount','project_teacher_id']]
#df[df.project_id == '68710b809ed5cecbbbb0272ec0fed5e2']['project_essay_text']
#drop no project descriptions
df = df.drop([1948656, 1948657,3466862])
#drop price outlier
df = df.drop([234121])
df['mdate'] = pd.to_datetime(df[['year', 'month']].assign(DAY=1))


additional_vars = ['mdate',
            'project_school_name',
            'project_school_id',
            'project_zip', 
            'project_city',
            'project_posted_date',
            'project_essay_text']

regression_df = df [['project_id']+ regression_vars + interest_vars + additional_vars]
regression_df.to_pickle('/scratch/gpfs/el1847/dec2022data/processed_data/regressiondf.pkl')
regression_df.to_csv("/scratch/gpfs/el1847/dec2022data/processed_data/regressiondf_pass_r_vars_only.csv")




