#!/usr/bin/env python
# coding: utf-8

import boto3,io,os,csv, sys
import boto3.session
import pandas as pd
from pyathena import connect as connectAthena
from pyathena.pandas.cursor import PandasCursor
import numpy as np
import codecs
import hashlib as hash
import time
import json
import requests
from datetime import timedelta
import datetime as dt
import logging
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.lineage import AUTO
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable


import pyodbc 
import pandas as pd 
import boto3 
import numpy as np
from sagemaker import get_execution_role
import warnings
warnings.filterwarnings("ignore")
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib import pyplot
from pandas import ExcelWriter
from pandas import ExcelFile
from plotly import __version__
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
import plotly.graph_objects as go
from textblob import TextBlob, Word, Blobber
from io import StringIO # python3; python2: BytesIO 
import nltk
nltk.download('punkt')
from nltk.corpus import stopwords
nltk.download("stopwords")
listsp=stopwords.words('english')
from dateutil.relativedelta import *
import re
import nltk
from nltk.corpus import words
nltk.download('tagsets')
#nltk.help.upenn_tagset()
from nltk.tokenize import sent_tokenize
from nltk.corpus import stopwords

import nltk.data
from nltk.tokenize import word_tokenize
nltk.download('wordnet')
from nltk.corpus import wordnet as wn
import spacy 
#nlp = spacy.load('en_core_web_md')
import operator
nltk.download('averaged_perceptron_tagger')
nlp = spacy.load("en_core_web_sm")
from nltk import word_tokenize, pos_tag
nltk.download('words')
import string
import time

startTime = time.localtime()
LOGGER = logging.getLogger()
lastDate = ''
aws_connection = Variable.get("AWS_DEV", deserialize_json=True)
aws_dev_session = boto3.session.Session(aws_access_key_id =aws_connection["Key"],
                                        aws_secret_access_key =aws_connection["Secret"],
                                        region_name = aws_connection["Region"])
aws_access_key_id =aws_connection["Key"]
aws_secret_access_key =aws_connection["Secret"]
region_name = aws_connection["Region"]

axon_session = boto3.Session(aws_access_key_id =aws_connection["Key"],
                        aws_secret_access_key =aws_connection["Secret"]
                        ,region_name='us-east-1')
dynamodb = axon_session.resource('dynamodb',  region_name='us-east-1')

azure_connection = Variable.get("AZURE", deserialize_json=True)
app_id = azure_connection["Key"]
client_secret = azure_connection["Secret"]
tenant_id = azure_connection["TenantID"] 


def Sagemaker_progressImport(session, app_id, client_secret, tenant_id, aws_access_key_id, aws_secret_access_key, region_name):
    

def determine_tense_input(sentence):
    text = word_tokenize(sentence)
    tagged = pos_tag(text)

    tense = {}
    tense["future"] = len([word for word in tagged if word[1] == "MD"])
    tense["present"] = len([word for word in tagged if word[1] in ["VBP", "VBZ","VBG"]])
    tense["past"] = len([word for word in tagged if word[1] in ["VBD", "VBN"]]) 
    domn_tense=max(tense.items(), key=operator.itemgetter(1))[0]
    if tense["future"]>0 :
        domn_tense='future'
  
    return(domn_tense)

conn = connectAthena( aws_access_key_id="AKIAT4NDQHGQYHXOHN5V",
                 aws_secret_access_key="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",
                s3_staging_dir="s3://cortica-athena-queries/leila/",
                region_name="us-east-1")

 


df = pd.read_sql_query('''  SELECT encd.key,encd.keyid,encd.encounterdataclob,en.clinicalencounterid,en.encounterdate,en.specialty,en.patientid,en.clinicalencountertype,en.providerid
FROM "athenacurrent"."clinicalencounterdata" encd
join "athenacurrent"."clinicalencounter" en on encd.clinicalencounterid=en.clinicalencounterid
join "athenacurrent"."patient" p on p.patientid=en.patientid
join "athenacurrent"."appointment" a on a."appointmentid" = en."appointmentid"
join "athenacurrent"."appointmenttype"  at on a."appointmenttypeid" = at."appointmenttypeid"


and key='EXAMFREETEXT_CLOB_PhysicalExam'
and length(en.deleteddatetime) =0
and length(encd.deleteddatetime) = 0
and length(closeddatetime) >0
and a.appointmentid = a.parentappointmentid
and a.appointmentstatus not in ('x - Cancelled','f - Filled','o - Open Slot')


''', conn)

 
now = pd.to_datetime("now")

df['encounterdate']=pd.to_datetime(df['encounterdate'])
all_notes=df[df['encounterdate']<now]

provider = pd.read_sql_query('''  SELECT * from "athenacurrent"."provider" 


''', conn)


def progress_score(x):
    cut_thresh=0.7
    
    if pd.isnull(x):
        return 
    
    #text=list(x)[0]
    
    x=re.sub('<[^>]+>', '', x) # remove anything between <>
    x=re.sub(r'http\S+', '', x) # removes url 
    x=re.sub(r'(?<=[.,])(?=[^\s])', r' ', x) # add space after , and .
    x=re.sub(r'\S*@\S*\s?', r' ', x) # remove email address
    x=x.lower()
    sents=sent_tokenize(x)

    keywordlist_negative=['scream','resistance', 'stimming', 'upset', 'crying', 'dysregulation', 'interruption','meltdowns','outbursts','vocalizations','distracted','support','repetitive','tapping','protest','hit','bit','bad','confused','hard']
    
    keywordlist_positive=['optimal arousal','regulation', 'engage','communicate', 'follow', 'express', 'type', 'accurate', 'coordination', 'inhibition', 'awareness', 'attention', 'gestures','independence','met','address','focus','relaxed', 'progress', 'improvement','independent']

    AllPositveScores=0
    exact_positivescores=[]
    
    AllNegativeScores=0
    exact_negativescores=[]
    SENT=[]


    for snts in sents:
        
        if determine_tense_input(snts)=='past' : # only consider past tense sentences
           
            snts=re.sub(r'[0-9]', '', snts) # remove numbers
            snts=snts.translate(str.maketrans('', '', string.punctuation)) # remove punctuation
            
            txt = TextBlob(snts)
            TextBlob_Sent=txt.sentiment.polarity
            
            
            if TextBlob_Sent <0:
                neg_score=TextBlob_Sent
                 # remove stopwords and check sentiment again
                    
                
                    
                text=re.sub('low', '', snts)  
                text=re.sub('not', '', text) 
                text=re.sub('less', '', text) 
                text=re.sub('more', '', text) 
                text=re.sub('no', '', text) 
                
                temp=word_tokenize(text) 
                filt_w=[w for w in temp if not w in listsp] 
                text=' '.join(filt_w)
                

                
                filt_snt = TextBlob(text)
                response =filt_snt.sentiment.polarity# sentiment without stopwords

            
                if ((response< 0.01) & (response >- 0.01))  :
                    clean_temp=word_tokenize(text) 

        
                    performance_score=[] 
    
                    for p in clean_temp:  # now check if it has a meaning similar to cortica +keywords
                              i=nlp(p)
                              if (i.vector_norm): # check to see if the word is meaningful
                                   for j in keywordlist_negative:
                                        M=nlp(j)
                                        tmp_score=i.similarity(M)
                                        performance_score.append(tmp_score)
                
                    if len(performance_score)>1:       
                        Max_perfm=max(performance_score)  
                        if Max_perfm>cut_thresh:
                             AllPositveScores=AllPositveScores+1
                             
                             exact_positivescores.append(Max_perfm)
                             SENT.append(snts)
  


                           
                                
                  
               
                                            
                if ((response>0.1)  |  (response<-0.1)):
                            Max_perfm=neg_score
                            exact_negativescores.append(Max_perfm)
                            AllNegativeScores=AllNegativeScores+1
                            SENT.append(snts)


            
        
                        
            elif TextBlob_Sent>0.1:
                
                       score_tmp=TextBlob_Sent
                
                       text=re.sub('low', ' ', snts) 
                       text=re.sub('high', ' ', text) 
                       text=re.sub('not', ' ', text) 
                       text=re.sub('less', ' ', text) 
                       text=re.sub('more', ' ', text) 
                    
                       temp=word_tokenize(text) 
                       filt_w=[w for w in temp if not w in listsp] 
                       text=' '.join(filt_w)
                
                       filt_snt = TextBlob(text)
                       response =filt_snt.sentiment.polarity# sentiment without stopwords

          

                       if ((response>-0.01) & (response<0.01)):
                            
                            
                             clean_temp=word_tokenize(text) 

                             performance_score=[] 
    
                             for p in clean_temp:  # now check if it has a meaning similar to cortica +keywords
                                   i=nlp(p)
                                   if (i.vector_norm): # check to see if the word is meaningful
                                       for j in keywordlist_negative:
                                            M=nlp(j)
                                            tmp_score=i.similarity(M)
                                            
                                            performance_score.append(tmp_score)
                
                                   if len(performance_score)>1:
                                      Max_perfm=max(performance_score)
                        
                                      if Max_perfm>cut_thresh:
                                        AllNegativeScores=AllNegativeScores+1
                                        SENT.append(snts)
                                    

     
                                
                       if ((response>0.01)  |  (response< -0.01)):
                            Max_perfm=score_tmp
                            exact_positivescores.append(Max_perfm)
                            AllPositveScores=AllPositveScores+1
                            SENT.append(snts+'144')
                            
                            
            elif (TextBlob_Sent> -0.01) & (TextBlob_Sent< 0.01):
                    performancepositive_score=[]
                    performancenegative_score=[]
                    
                    text=re.sub('low', '', snts) 
                    text=re.sub('high', '', text) 
                    text=re.sub('not', '', text) 
                    text=re.sub('less', '', text) 
                    text=re.sub('more', '', text) 
                    
                    temp=word_tokenize(text) 
                    filt_w=[w for w in temp if not w in listsp] 
                    text=' '.join(filt_w)
                    
                    clean_temp=word_tokenize(text) 

                    for p in clean_temp:  # now check if it has a meaning similar to cortica +keywords
                        i=nlp(p)
                        if (i.vector_norm): # check to see if the word is meaningful
                            for j in keywordlist_positive:
                                M=nlp(j)
                                p_score=i.similarity(M)
                                performancepositive_score.append(p_score)
                                #print(p,j,p_score)
                                
                                    
                                            
                                for s in keywordlist_negative:
                                    D=nlp(s)
                                    n_score=i.similarity(D)
                                    #print(s,i,n_score)
                                    performancenegative_score.append(n_score)
                                    
                    
                
                
                    if len(performancepositive_score)>1:
                        Maxp_perfm=max(performancepositive_score) 
                        if Maxp_perfm>cut_thresh:
                            AllPositveScores=AllPositveScores+1
                            exact_positivescores.append(Maxp_perfm)
                            SENT.append(snts+'182')
                        
                     
                    if len(performancenegative_score)>1:
                         Maxn_perfm=max(performancenegative_score)  
                         if Maxn_perfm>cut_thresh:
                              AllNegativeScores=AllNegativeScores+1
                              exact_negativescores.append(Maxn_perfm)
                              SENT.append(snts+'190')


                                                            
    final_score=[]
    #max(exact_positivescores),max(exact_negativescores),AllPositveScores,AllNegativeScores
    final_score={'positive count' :AllPositveScores,'positive score':exact_positivescores,'negative count':AllNegativeScores,'negative score':exact_negativescores,'sent':SENT}
    #return pd.Series([AllPositveScores,AllNegativeScores,SENT], index=['a', 'b', 'c'])
    return final_score

 

provider['providerid']=provider['providerid'].astype('int')
Final_df=provider[['providertype','providertypename','providertypecategory','providerid']].merge(all_notes,on='providerid')
GROUP_A=Final_df[Final_df['providertype'].isin(['OTA','DTP', 'SLP','OT','SLPA','PTA','DPT','PT'])]
last_enc=GROUP_A[['patientid','clinicalencounterid','key','keyid','encounterdataclob','encounterdate']].sort_values('keyid',ascending=False).groupby(['patientid','encounterdate','clinicalencounterid','key']).nth(0).reset_index()

all_ids=list(last_enc['patientid'].unique())[1:]
for pid in all_ids:

    t = time.time()
    df=last_enc[last_enc['patientid']==pid]
    print(pid,df.shape[0])
    dts=list(df['encounterdate'].unique())
    for DATE  in dts:
            temp=df[df['encounterdate']==DATE]
            M=temp['encounterdataclob'].apply(lambda x: progress_score(x))
            temp['positive_count']=M[M.notnull()].apply(lambda x:x['positive count'])
            temp['negative_count']=M[M.notnull()].apply(lambda x:x['negative count'])
            bucket = 'cortica-dl-sagemaker' 
            csv_buffer = StringIO()
            temp.to_csv(csv_buffer)
            s3_resource = boto3.resource('s3')
            s3_resource.Object(bucket, 'DT_note_processed/'+str(pid)+str(DATE) + 'temp.csv').put(Body=csv_buffer.getvalue())
            time.time() - t

last_enc['positive_count']=M[M.notnull()].apply(lambda x:x['positive count'])
last_enc['negative_count']=M[M.notnull()].apply(lambda x:x['negative count'])





                


DEFAULT_ARGS = {
    'owner': 'Leila',
    'depends_on_past': False,
    'email': ['lfakhraei@corticacare.com','rolson@corticacare.com'],
    'email_on_failure': True,
    'email_on_retry': False ,
}

with DAG(
    dag_id='SageMaker',
    default_args=DEFAULT_ARGS,
    schedule_interval='0 12 * * *',
    start_date=dt.datetime(2021, 4, 23, 12, 00),
    catchup=False,
    dagrun_timeout=timedelta(minutes=90),
    tags=['Progress'],
) as dag_1:
    Start = DummyOperator(task_id='Start')

    Sagemaker_progress = PythonOperator(task_id= "Sagemaker_progress", task_concurrency=1,  python_callable=Sagemaker_progressImport, op_kwargs={"aws_access_key_id":aws_access_key_id,"aws_secret_access_key":aws_secret_access_key,"region_name":region_name,"session":aws_dev_session, "app_id":app_id, "client_secret":client_secret, "tenant_id":tenant_id})
    
    End = DummyOperator(task_id='End')

    
        
