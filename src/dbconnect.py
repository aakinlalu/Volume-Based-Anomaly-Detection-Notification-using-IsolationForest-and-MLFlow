
import configparser
import sys
import os
import re
import pandas as pd

import psycopg2

from ua_parser import user_agent_parser


class Connect:
    '''
    Class Connect takes attrubute (sql query) and has two functions
    '''
    def __init__(self, template, config_path, which_database):
        self.template = template
        self.config_path = config_path
        self.which_database = which_database

    def read_config(self):
        try:
            if os.path.exists(self.config_path):
                parser = configparser.ConfigParser()
            else:
                print("Config not found! Exiting!")
                sys.exit(1)
            parser.read(self.config_path)
            host = parser.get(self.which_database, 'host')
            user =parser.get(self.which_database,'user')
            password=parser.get(self.which_database,'password')
            dbname=parser.get(self.which_database,'dbname')
            port=parser.get(self.which_database,'port')
            return host, user, password, dbname, port
        except OSError as e:
            return f'Exception Error: {e}'
        
        

    def dbconnector(self, column_to_parse):
        '''
        Parameters:
        -----------
        column_to_parse: object
        '''
            
        host, user, password, dbname, port=self.read_config()
            
        try:
            conn = psycopg2.connect(dbname=dbname, host=host, port=int(port), user=user, password=password)
            print(f'Successfully connected to the database on host: {host}')
            
            df = pd.read_sql(self.template, con=conn, parse_dates=[column_to_parse])
            if len(df)==0:
                print(df.head())
                print('Data has not been landed or processed yet')
                print(df.head())
            return df
            
        except psycopg2.DatabaseError as e:
            print(f'database error: {e}')
            return f'database error: {e}'
               
        except Exception as e:
            print(f'database error: {e}')
            return f'Other exception error: {e}'
        #finally:
            #df = pd.read_sql(self.template, con=conn, parse_dates=[column_to_parse])
                #df = df.set_index('date')
            #return df

        
    
    def transform_browser(self, df, user_agent_feature):
        '''
        The function will take dataframe and feature as arguments and
        parse feature to generate OS and add to the df
        
        Parameters:
        ----------
        df: pd.DataFrame
        user_agent_feature: str
        
        Return
        ------
        df
        '''
        try:
            for colname in df.columns:
                if type(df[colname])=='object':
                    df[colname]=df[colname].fillna('Other')
                    
            df[user_agent_feature] = df[user_agent_feature].apply(lambda x: 'Other' if x is None else x)
            df[user_agent_feature] = df[user_agent_feature].apply(lambda x:user_agent_parser.Parse(x)['os']['family'])
            
            list_of_features = list(df.columns)
            
            groupby_list = list_of_features[:-1]
            value_list = list_of_features[-1]
            
            new_df = df.groupby(groupby_list)[value_list].sum().reset_index()
            return new_df
            
        except ValueError as e:
            print(f'Value error is: {e}')
            return f'Value error is: {e}'
        
        except Exception as e:
            print(f'Other error is: {e}')
            return f'Other error is: {e}'
        #finally:
            #return new_df

    