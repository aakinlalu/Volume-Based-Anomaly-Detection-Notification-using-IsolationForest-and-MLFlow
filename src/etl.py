import os
import warnings
import sys
import logging
import re


import numpy as np
import pandas as pd

import sklearn
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import LabelEncoder


class Transformer:
    """
    The Class does not have any attribute but has two functions:
       text_encoding
       data_prep
       
    """

    @staticmethod    
    def text_encoding(df):
        '''
        The function uses label-endcoding to vectorize any text variable in the dataframe
        Parameters:
        -----------
        df: pd.DataFrame
        
        Return
        ------
        df: pd.DataFrame
        '''
       
        try:
            dic = dict()
            if type(df)==pd.core.frame.DataFrame:
                for colname in df.columns:
                    if df[colname].dtype == "object":
                        dic[colname] = LabelEncoder()
                        df[colname] = dic[colname].fit_transform(df[colname])
                return df
                
            else:
                print("The input data is not dataframe")
                
        except Exception as e:
            return f'Other exceptions {e}'
             
    
    @staticmethod    
    def data_prep(df):
        '''
        The function takes dataframe as argument and standarding the dataset then return dataframe
        Parameters
        ----------
        df: pd.DataFrame
        
        Return
        ------
        df: pd.DataFrame
        '''
        
        try:
            if type(df)==pd.core.frame.DataFrame:
                list_of_features = list(df.columns)
                scaler = StandardScaler()
                data_scaler = scaler.fit_transform(df)
                data = pd.DataFrame(data_scaler, columns=list_of_features, index=df.index)
                return data
            else:
                print("The input data is not dataframe")
        except Exception as e:
            print(f'Other exception {e}')
            return f'Other exception {e}'
        
            
      
    @staticmethod
    def categorise(df, colname):
        '''
        The function takes two arguments and create nomimal categories
        Parameters:
        -----------
        df: pd.DataFrame
        colname: str
        
        Return
        -----
        df: pd.Dataframe
        '''
        try:
            feature_type = df[colname].unique()
            df[colname] = df[colname].astype('category', categories=feature_type)
            return df 
        except ValueError as e:
            print(f'Value error {e}')
            return f'Value error {e}'
            
    
    @staticmethod
    def add_columns(df, index_name):
        '''
        The function takes two arguments and expect second arguument to be date.
        It sets new index for the dataframe and generate weekday and month columns
        '''
        try:
            if type(df)==pd.core.frame.DataFrame and df[index_name].dtype =="datetime64[ns]":
                new_df = df.set_index(index_name)
                new_df['weekday'] = new_df.index.weekday
                new_df['month'] = new_df.index.month
            else:
                print("The input data is not dataframe")
               
        except ValueError as e:
            print(f'Value error {e}')
            return f'Value error {e}'
        finally:
             return new_df 
            
    
    @staticmethod
    def train_outlier_nomal(data, outlier_filter, normal_filter):
        '''
          The function takes three arguments and return train, asummed outlier data and normal data
          Parameters:
          ----------
           data: pd.DataFrame
           outlier_filter: List
           normal_filter: List
    
          Returns:
          --------
          traindata: pd.DataFrame
          outlier: pd.DataFrame
          normal: pd.DataFrame
         '''
        try:
            outlier_df = data.loc[outlier_filter]
            normal_df = data.loc[normal_filter]
        
            traindata = data.drop(index=outlier_filter, axis=1)
            traindata.drop(index=normal_filter, axis=1, inplace=True)
        
            outlier_df['label'] = -1
            normal_df['label'] = 1
        
            y_outlier = outlier_df['label']
            x_outlier =  outlier_df.drop('label', axis=1)
        
            y_normal = normal_df['label']
            x_normal = normal_df.drop('label', axis=1)
        
            return traindata, x_outlier, y_outlier, x_normal, y_normal 
    
        except ValueError as e:
            print(f'ValueError is {e}')
            return f'ValueError is {e}'
    
        