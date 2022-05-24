import pandas as pd
import numpy as np
import sys
sys.path.insert(0,'../scripts/')
missing_values = ["n/a", "na", "undefined",'']
from data_loader import load_df_from_csv

class DataCleaner:
    def __init__(self, df: pd.DataFrame) -> None:
        """
        Returns a DataCleaner Object with the passed DataFrame Data set as its own DataFrame
        Parameters
        ----------
        df:
            Type: pd.DataFrame
        Returns
        -------
        None
        """
        self.df = df   
    def remove_unwanted_columns(self, columns: list) -> pd.DataFrame:
        """
        Returns a DataFrame where the specified columns in the list are removed
        Parameters
        ----------
        columns:
            Type: list
        Returns
        -------
        pd.DataFrame
        """
        self.df.drop(columns, axis=1, inplace=True)
        return self.df
    ## Convert the Tabular Data to Time Series Data  
    def table_to_time_series(self,column,date_column):
        """
        - transform the data into a 
        time series dataset
        """
        self.df.sort_values([column,date_column], ignore_index=True, inplace=True)
        #sequential ordering of events
        self.df[date_column] = pd.to_datetime(self.df[date_column],errors='coerce')
        self.df['Day'] = self.df[date_column].dt.day
        self.df['Month'] = self.df[date_column].dt.month
        self.df['Year'] = self.df[date_column].dt.year
        self.df['DayOfYear'] = self.df[date_column].dt.dayofyear
        self.df['WeekOfYear'] = self.df[date_column].dt.weekofyear
        self.df.set_index(date_column, inplace=True)
    def remove_unnamed_cols(self):
        """
        - this algorithm removes columns with unnamed ad id from test
        """
        self.df.drop(self.df.columns[self.df.columns.str.contains('unnamed',case = False)],axis = 1, inplace = True)
        self.df.drop(self.df.columns[self.df.columns.str.contains('Id',case = False)],axis = 1, inplace = True)
    def create_new_columns_from(self, new_col_name: str, col1: str, col2: str, func) -> pd.DataFrame:
        """
        Returns a DataFrame where a new column is created using a function on two specified columns
        Parameters
        ----------
        new_col_name:
            Type: str
        col1:
            Type: str
        col2:
            Type: str
        func:
            Type: function
        Returns
        -------
        pd.DataFrame
        """
        try:
            self.df[new_col_name] = func(self.df[col1], self.df[col2])
        except:
            print("failed to create new column with the specified function")

        return self.df
    def merge_store_df(self,df_,column):
        """
        expects:
            - string(column)
        returns:
            - merged df
        """
        self.df = pd.merge(self.df, df_, how = 'inner', on = column)
        return self.df
    def save_clean_data(self, name: str):
        """
        The objects dataframe gets saved with the specified name 
        Parameters
        ----------
        name:
            Type: str
        Returns
        -------
        None
        """
        try:
            self.df.to_csv(name)

        except:
            print("Failed to save data")
            
    def return_df(self):
        """
        - returns the dataframe
        """
        return self.df
if __name__ == '__main__':
    store = load_df_from_csv("../data/train.csv",na_values=missing_values)
    df = load_df_from_csv("../data/train.csv",na_values=missing_values)
    clean_df = DataCleaner(df)
    #clean_df.merge_store_df(store,'Store')
    clean_df.remove_unnamed_cols()
    clean_df.table_to_time_series("Store","Date")
    clean_df.save_clean_data(name="data/clean_train.csv")
    
    test = load_df_from_csv("../data/test.csv",na_values=missing_values)
    clean_df = DataCleaner(test)
    #clean_df.merge_store_df(store,'Store')
    clean_df.remove_unnamed_cols()
    clean_df.table_to_time_series("Store","Date")
    clean_df.return_df().drop('Id',axis=1,inplace=True)
    clean_df.save_clean_data(name="data/clean_test.csv")
    '''
    train = train[(train["Open"] != 0) & (train['Sales'] != 0)]
    train = train.reset_index(drop=True) 
    
    store['CompetitionDistance'].fillna(store['CompetitionDistance'].median(), inplace = True)
    # replace NA's by 0
    store.fillna(0, inplace = True)
    
    '''
    
    


