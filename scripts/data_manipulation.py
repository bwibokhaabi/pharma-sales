import pandas as pd
import numpy as np


class DataInfo:
    def __init__(self, df: pd.DataFrame):
        """
            Returns a DataInfo Object with the passed DataFrame Data set as its own DataFrame
            Parameters
            ----------
            df:
                Type: pd.DataFrame
            Returns
            -------
            None
        """
        self.df = df

    def get_basic_description(self):
        """
            Runs get_size, get_total_memory_usage, get_memory_usage and get_information
            Parameters
            ----------
            None
            Returns
            -------
            None
        """
        self.get_size()
        self.get_total_memory_usage()
        self.get_memory_usage()
        self.get_information()

    def get_missing_description(self):
        """
            Runs get_total_missing_values, get_columns_with_missing_values and get_column_based_missing_values
            Parameters
            ----------
            None
            Returns
            -------
            None
        """
        self.get_total_missing_values()
        self.get_columns_with_missing_values()
        self.get_column_based_missing_values()

    def get_columns(self):
        """
            prints and returns columns of the objects dataframe
            Parameters
            ----------
            None
            Returns
            -------
            None
        """
        print("Columns Listed in the DataFrame are: ")
        return self.df.columns.tolist()

    def get_information(self):
        """
            prints and returns the info of the dataframe
            Parameters
            ----------
            None
            Returns
            -------
            None
        """
        print("DataFrame Information: ")
        return self.df.info()

    def get_size(self):
        """
            prints and returns the size of the dataframe
            Parameters
            ----------
            None
            Returns
            -------
            None
        """
        value = self.df.shape
        print(
            f"The DataFrame containes {value[0]} rows and {value[1]} columns.")
        return value

    def get_total_entries(self):
        """
            prints and returns the total entries of the dataframe
            Parameters
            ----------
            None
            Returns
            -------
            None
        """
        value = self.df.size
        print(f"The DataFrame containes {value} entries.")
        return value

    def get_description(self):
        """
            returns the description of the dataframe
            Parameters
            ----------
            None
            Returns
            -------
            None
        """
        return self.df.describe()

    def get_dispersion_params(self) -> pd.DataFrame:
        """
            returns the description plus mode and median of the dataframe
            Parameters
            ----------
            None
            Returns
            -------
            None
        """
        return self.df.describe().append(self.get_mode()).append(self.get_median()).dropna(1)

    def get_column_dispersion_with_total_params(self) -> pd.DataFrame:
        """
            returns the description plus mode, median and total of the dataframe
            Parameters
            ----------
            None
            Returns
            -------
            None
        """
        return self.df.describe().append(self.get_mode()).append(self.get_median()).append(self.get_total()).dropna(1)

    def get_column_dispersion_params(self, col: str) -> pd.DataFrame:
        """
            returns the description plus mode and median of a specified column in the dataframe
            Parameters
            ----------
            col:
                Type: str
            Returns
            -------
            None
        """
        return self.df.describe().append(self.get_mode()).append(self.get_median()).dropna(1)[col]

    def get_total(self):
        """
            returns the total of each column of the dataframe
            Parameters
            ----------
            None
            Returns
            -------
            None
        """
        total = self.df.sum()
        total.name = 'Total'
        return total

    def get_mode(self):
        """
            returns the mode of each column of the dataframe
            Parameters
            ----------
            None
            Returns
            -------
            None
        """
        mode = self.df.mode()
        actual_mode = mode.iloc[0, :]
        actual_mode.name = 'Mode'
        return actual_mode

    def get_median(self):
        """
            returns the median of each column of the dataframe
            Parameters
            ----------
            None
            Returns
            -------
            None
        """
        median = self.df.median()
        median.name = 'Median'
        return median

    def get_column_description(self, column_name: str):
        """
            returns the description of a column in the dataframe
            Parameters
            ----------
            column_name:
                Type: str
            Returns
            -------
            None
        """
        try:
            return self.df[column_name].describe()
        except:
            print("Failed to get decription of the column")

    def get_mean(self):
        """
            returns the mean of each column of the dataframe
            Parameters
            ----------
            None
            Returns
            -------
            None
        """
        return self.df.mean()

    def get_column_mean(self, column_name: str):
        """
            returns the mean of the specified column in the dataframe
            Parameters
            ----------
            column_name:
                Type: str
            Returns
            -------
            None
        """
        try:
            return self.df[column_name].mean()
        except:
            print("Failed to get decription of the column")

    def get_standard_deviation(self):
        """
            returns the standard deviation of each column of the dataframe
            Parameters
            ----------
            None
            Returns
            -------
            None
        """
        return self.df.std()

    def get_column_standard_deviation(self, column_name: str):
        """
            returns the standard deviation of the specified column in the dataframe
            Parameters
            ----------
            column_name:
                Type: str
            Returns
            -------
            None
        """
        try:
            return self.df[column_name].std()
        except:
            print("Failed to get decription of the column")

    def get_total_missing_values(self):
        missing_entries = self.df.isnull().sum().sum()
        total_entries = self.df.size
        print(f"The total number of missing values is {missing_entries}")
        print(round(((missing_entries/total_entries) * 100), 2),
              "%", "missing values.")
        return missing_entries

    def get_columns_with_missing_values(self):
        lst = self.df.isnull().any()
        arr = []
        index = 0
        for col in lst:
            if col == True:
                arr.append(lst.index[index])
            index += 1
        return arr

    def get_column_based_missing_values(self):
        value = self.df.isnull().sum()
        df = pd.DataFrame(value, columns=['missing_count'])
        df.drop(df[df['missing_count'] == 0].index, inplace=True)
        df['type'] = [self.df.dtypes.loc[i] for i in df.index]
        return df

    def get_column_based_missing_percentage(self):
        col_null = self.df.isnull().sum()
        total_entries = self.df.shape[0]
        missing_percentage = []
        for col_missing_entries in col_null:
            value = str(
                round(((col_missing_entries/total_entries) * 100), 2)) + " %"
            missing_percentage.append(value)

        missing_df = pd.DataFrame(col_null, columns=['total_missing_values'])
        missing_df['missing_percentage'] = missing_percentage
        return missing_df

    def get_columns_missing_percentage_greater_than(self, num: float):
        all_cols = self.get_column_based_missing_percentage()
        extracted = all_cols['missing_percentage'].str.extract(r'(.+)%')
        return extracted[extracted[0].apply(lambda x: float(x) >= num)].index

    def get_duplicates(self):
        return self.df[self.df.duplicated()]

    def get_total_memory_usage(self):
        value = self.df.memory_usage(deep=True).sum()
        print(f"Current DataFrame Memory Usage:\n{value}")
        return value

    def get_memory_usage(self):
        print(f"Current DataFrame Memory Usage of columns is :")
        return self.df.memory_usage()

    def get_aggregate(self, stat_list: list):
        try:
            return self.df.agg(stat_list)
        except:
            print("Failed to get aggregates")

    def get_matrix_correlation(self):
        return self.df.corr()

    def get_grouped_by(self, column_name: str):
        try:
            return self.df.groupby(column_name)
        except:
            print("Failed to get grouping column")

    def get_col_unique_value_count(self, col):
        try:
            print(
                f'Number of unique values in column {col} is: {str(len(self.df[col].unique()))}')
        except:
            print('Failed to get unique values')

    def get_dataframe_columns_unique_value_count(self):
        return pd.DataFrame(self.df.apply(lambda x: len(x.value_counts(dropna=False)), axis=0), columns=['Unique Value Count']).sort_values(by='Unique Value Count', ascending=True)

    