#---------------------------Import Inbuilt packages---------------------------------
import os
import sys
import csv
from datetime import datetime, timedelta


import pandas as pd
import numpy as np

import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot as plt
import seaborn as sns

#--------------------------Installed packages------------------------------------------

import mlflow
import mlflow.sklearn
from mlflow.tracking import MlflowClient
from sklearn.externals import joblib


#-----------------------Homemade packages----------------------------------------------
from src.etl import Transformer
from src.notification import Notification as notify
from src.dbconnect import Connect
from src.logfile import log


def store_run_df(experiment_name, experiment_id):
    client =MlflowClient()
    if client.list_experiments()[0].name ==experiment_name:
        run_df = pd.DataFrame([(run.run_uuid, run.start_time, run.artifact_uri) for run in client.list_run_infos(experiment_id)])
        run_df.columns = ['run_uuid', 'start_time', 'artifact_uri']
        run_df['start_time'] = pd.to_datetime(run_df['start_time'], unit='ms')
        run_df =run_df.sort_values("start_time", ascending=False)
        run_df['train_accuracy'] = [client.get_run(run_df.loc[i]['run_uuid']).data.metrics['train_accuracy']
                                    if len(client.get_run(run_df.loc[i]['run_uuid']).data.metrics)>0 else 0 for i in range(len(run_df))]
        run_df['test_accuracy'] = [client.get_run(run_df.loc[i]['run_uuid']).data.metrics['test_accuracy']
                         if len(client.get_run(run_df.loc[i]['run_uuid']).data.metrics)>0 else 0 for i in range(len(run_df))]
        return run_df


def percent_of_contamination(scores, comtamination):
    try:
        contamination_points = [item for item in scores if item <=-comtamination]
        percent_contamination = (len(contamination_points)/len(scores)) * 100
        return percent_contamination
    except ValueError as e:
        print(f'Errors is {e}')

def decision_chart(scores, contamination, savepath):
    fig = plt.figure(figsize=(10,4))
    plt.title("decision function score")
    plt.hist(scores, bins='auto', alpha=0.7, color='#0504aa', rwidth=0.85)
    plt.axvline(x=-contamination, color='r', linestyle='--', label="Contamination point")
    plt.text(-contamination-0.005, 3000,  'Contamination zone', rotation=90)
    plt.savefig(savepath)
    plt.close()


@log
def main():
    #---------------- Define your Email parameters and recipients -----------------------

    #Email parameters

    host=''
    author=''

    recipient0=""
    recipient1=""
    recipient2=""
    recipient3=""
    recipient4=""
    recipient5=""


    table_name = ""

    EmailNotify0 = notify(host=host,from_addr=author,to_addr=recipient0, table=table_name)
    EmailNotify1 = notify(host=host,from_addr=author,to_addr=recipient1, table=table_name)
    EmailNotify2 = notify(host=host,from_addr=author,to_addr=recipient2, table=table_name)
    EmailNotify3 = notify(host=host,from_addr=author,to_addr=recipient3, table=table_name)
    EmailNotify4 = notify(host=host,from_addr=author,to_addr=recipient4, table=table_name)
    EmailNotify5 = notify(host=host,from_addr=author,to_addr=recipient5, table=table_name)


    #------------------------Sourcing the dataset from db and conforming it----------------------------
    #SQL temeplate
    template = """

            """

    #Database Parameters
    config_path = ""
    which_database = "Redshift_prod"


    subject =f'Issues with {table_name} table'           #------Email Subject

    source=Connect(template, config_path, which_database)
    df = source.dbconnector('date')

    if len(df)>0:                                         #-----Dataframe should not be empty
        df = source.transform_browser(df, 'os_family')    #------
    else:                                                 #---Dataframe is empty then alert recipeients
        #-- send email about
        EmailNotify0.send_email(subject)
        EmailNotify1.send_email(subject)
        EmailNotify2.send_email(subject)
        EmailNotify3.send_email(subject)
        EmailNotify4.send_email(subject)
        EmailNotify5.send_email(subject)
        sys.exit(0)                                       #----exit the system



    #-------------------------Apply ETL to tramsform the dataset to fit for Trainer-----------------------------------
    copy_df = df.copy()

    Etl_for_ml = Transformer()                            #--- Call Transformer class
    copy_df = Etl_for_ml.add_columns(copy_df,'date')      #---- Change date column into index and create extract columns weekdays and month

    new_df=  Etl_for_ml.text_encoding(copy_df)            #---- Use Labelencoder to encode every text variables in dataframe as integer

    data = Etl_for_ml.data_prep(new_df)                   #----- Standards the



    #---------------------------------model and Predict-----------------------------
    contamination = 0.07                  #------ % of Contamination

    client =MlflowClient()

    run_df = store_run_df('IB', '0')                      #----- Create dataframe for experiment_name=IB and experiment_id=0


    #artifact_fact_url= run_df.loc[1]['artifact_uri']     #-------- Find the url for aritifact
    #location = client.get_experiment('0').artifact_location
    #runid = 'bedc2e0de2fd4f24b9e60bac8853abe2'

    url = 'mlruns/0/bedc2e0de2fd4f24b9e60bac8853abe2/artifacts/ib-isolationforest-model'

    #modelurl = os.path.join(artifact_fact_url, 'ib-isolationforest-model')   #---- Create model url

    model = mlflow.sklearn.load_model(model_uri=url)                    #------Load the model


    scores = model.decision_function(data)          #----- Probability Scores


    percent_contamination = percent_of_contamination(scores, contamination)



    if os.path.exists('Resultdir') == False:  #--Check if Resultdir does not exist
            os.mkdir('Resultdir')             #--- Then Create it

    df['predictions'] = model.predict(data)  #----Predict and set the predictions as columns in the dataframe
    df['scores'] = scores
    anomaly_df = df[df['predictions']==-1]             #---Identify anomaly


    date = anomaly_df.iloc[0]['date']             #----- Pick a date from anomaly dataframe


    # Create and lot plot based on decision scores
    filename = '_'.join(['anomaly', str(date.year), str(date.month), str(date.day)])   #---Create file name

    imagepath = '.'.join([filename, 'png'])
    figpath =  'Resultdir' +'/'+ imagepath
    decision_chart(scores, contamination, figpath)


    if len(anomaly_df)>0 and percent_contamination > (contamination*100):                             #-----If anomaly dataframe is not empty

        csvpath = '.'.join([filename, 'csv'])
        filepath =os.path.join('Resultdir', csvpath)      #------ set url for the filename
        anomaly_df.to_csv(filepath, index=False)          #--------Write the dataframe into csv


        #-----------------------Send email-----------------------------------------------
        EmailNotify0.send_email_2(subject,figpath)
        EmailNotify1.send_email_2(subject,figpath)
        EmailNotify2.send_email_2(subject,figpath)
        EmailNotify3.send_email_2(subject, figpath)
        EmailNotify4.send_email_2(subject, figpath)
        EmailNotify5.send_email_2(subject, figpath)
    else:
        sys.exit(0)

if __name__=='__main__':
    main()
