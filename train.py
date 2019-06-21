import warnings
import sys


import pandas as pd
import numpy as np

from sklearn.metrics import accuracy_score, roc_auc_score
from sklearn.metrics import confusion_matrix,f1_score, classification_report

from sklearn.preprocessing import StandardScaler

from sklearn.ensemble import IsolationForest


import matplotlib.pyplot as plt
import seaborn as sns

import mlflow
import mlflow.sklearn

from etl import Transformer

def eval_metrics(actual, pred):
    accuracy = round(accuracy_score(actual, pred),2)
    fiscore = round(f1_score(actual, pred),2)
    return accuracy, fiscore


def classification_report_df(class_report):
    class_report_list = []
    for item in class_report.split('\n'):
        if item != '':
            class_report_list.append([i.strip() for i in item.strip().split(' ') if i != ''])
            
    class_report_list[0].insert(0, 'class')
    class_report_list[0].insert(0, '')
    
    for item in class_report_list[1:len(class_report_list)-3]:
        item.insert(0,'')
        
    df=pd.DataFrame(class_report_list[1:], columns=class_report_list[0])
    
    return df


def log_anomaly(experimentID, run_name, params, traindata,  testdata):
    
    warnings.filterwarnings("ignore")
    np.random.seed(40)
    
    with mlflow.start_run(experiment_id=experimentID, run_name=run_name) as run:
        # Create model, train it, and create predictions
        iForest = IsolationForest(**params)
    
        iForest.fit(traindata)
        
        ##Predict with train data
        train_predictions = iForest.predict(traindata)
        test_predictions = iForest.predict(testdata)

        # Log model
        mlflow.sklearn.log_model(iForest, "ib-isolationforest-model")

        # Log params
        [mlflow.log_param(param, value) for param, value in params.items()]
        
        #Accuracy Metrics
        train_accuracy = round(list(train_predictions).count(1)/train_predictions.shape[0],2)  #create train accuracy metrics

        test_accuracy = round(list(test_predictions).count(1)/test_predictions.shape[0],2)  #create test accuracy metrics
        
        print('Accuracy Metrics:')
        print('--------------------------------------------')
        print(f'Accuracy for train data: {train_accuracy}')
        print(f'Accuracy for test data: {test_accuracy}')
        print(' ')
        
        # Log metrics
        mlflow.log_metric("train_accuracy", train_accuracy)
        mlflow.log_metric("test_accuracy", test_accuracy) 
       
        #Log Artifact
        training_scores = iForest.decision_function(traindata)
        test_scores = iForest.decision_function(testdata)
    

        # Create and lot plot
        fig, (ax1, ax2) = plt.subplots(2, 1, sharex=True)
        
        #fig = plt.figure(figsize=(10,8))
        title = fig.suptitle("Decision Function Score", fontsize=14)
        fig.subplots_adjust(top=0.85, wspace=0.3)
    
    
        #ax = fig.add_subplot(1,1,1)
        ax1.set_title("Decision function score for trainingdata")
        ax1.hist(training_scores, bins='auto', alpha=0.7, color='#0504aa', rwidth=0.85)
        
        #ax1 = fig.add_subplot(2,1,1)
        ax2.set_title("Decision function score for testdata")
        ax2.hist(test_scores, bins='auto', alpha=0.7, color='#0504aa', rwidth=0.85)
        
        #create Confusion Matrix
        #cm = confusion_matrix(y_test, test_predictions)
        #sns.heatmap(cm, annot=True, cmap="coolwarm", fmt="d", linewidths=.5, ax=ax1)
        
        n_id = run.info.run_uuid
        
        table_name = 'activityhistory'
        
        
        fig_path = 'data'+ '/'+'decisionfunction'+'/'+table_name + '_' + str(n_id) + '.png'

        fig.savefig(fig_path)

        mlflow.log_artifact(fig_path)
        
        print(fig)
   
        return  f'RunID:{n_id}'