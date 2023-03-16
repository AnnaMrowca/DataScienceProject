import pandas as pd
from parser import ParserError
import numpy as np



class DataLoader():
    def __init__(self,
                 input_data_path: str,
                 sep: str = ',',
                 decimal: str = '.',
                 csv_format_path: str = ' ',
                 coefficient_nulls_removal: int = 50,
                 date_column_name: list = [],
                 datetime_duplicated_column_name: list = [],
                 multi_index_columns: list = [],
                 date_duplicate_column_name: list =[],
                 outliers_columns: list =[],
                 ):
        self.input_data_path = input_data_path
        self.sep = sep
        self.decimal = decimal
        self.csv_format_path = csv_format_path
        self.coefficient_nulls_removal = coefficient_nulls_removal
        self.date_column_name = date_column_name
        self.datetime_duplicated_column_name = datetime_duplicated_column_name
        self.multi_index_columns = multi_index_columns
        self.date_duplicate_column_name = date_duplicate_column_name
        self.outliers_columns = outliers_columns

    def get_initial_data(self):

        """
         This function returns initial data for further clean-up.
         If dataset is has txt extension, it turns it into csv.

        """
        if self.input_data_path.endswith('txt'):
            initial_data = pd.read_csv(self.input_data_path)
            initial_data.to_csv(self.csv_format_path, header=None, index=False, sep=self.sep, decimal=self.decimal)
            return initial_data
        else:
            index_col = []
            initial_data = pd.read_csv(self.input_data_path, sep=self.sep, decimal=self.decimal, index_col=index_col)
            return initial_data

    def remove_missing_data(self, data):

        """
        This function checks initial state of nulls. Next, it zips column name with sum of nulls.
        If percentage of sum of nulls is bigger than coefficient_nulls_removal the data is removed.
        E.g. if coefficient_nulls removal is equal to 50.
        It means that if column does not have 50% or more data it will be removed

        """
        print(f'Check nulls before\n{data.isnull().sum()}')

        to_remove = []
        for (col_name, no_of_nulls) in zip(data.isnull(), data.isnull().sum()):
            if self.coefficient_nulls_removal <= (no_of_nulls / len(data)) * 100:
                to_remove.append(col_name)
        data.drop(to_remove, inplace=True, axis=1)

        print(f'Check nulls after\n{data.isnull().sum()}')
        return data

    def indicate_duplicated_columns(self, data):

        """
        This function returns list of duplicated columns. When column names are duplicated, DataFrame creates
        duplicate by adding "." and number, e.g. 1. That is why we can distinguish duplicates by eliminating names with "."
        Currently, the code works only when names of columns do not contain dot - ".". If more complicated naming occurs,
        this function needs to be adjusted

        """
        non_duplicate_columns_names = set()
        duplicate_columns_names = set()

        index = 0

        for column in data.columns:
            if '.' not in column:
                non_duplicate_columns_names.add(column)
            else:
                duplicate_columns_names.add(column)
            index += 1
        return list(duplicate_columns_names)

    def remove_duplicated_columns(self, data):

        """
        This function removes duplicated columns using list of duplicated columns names

        """

        duplicate_columns_names = self.indicate_duplicated_columns(data)
        data = data.drop(columns=duplicate_columns_names, axis=1)
        return data

    def handle_multi_index(self, data):

        """
        This function returns True if DataFrame is MultiIndex or False if is not

        """

        if isinstance(data.index, pd.MultiIndex):
            data = data.reset_index(self.multi_index_columns)
            print(data)
            return data
        else:
            print('File is not a multiindex')
            return data

    def delete_duplicated_rows(self, data):

        """
        This function drops duplicated rows, keeping records that show up first in the dataset

        """

        data = data.drop_duplicates(keep='first')
        return data

    def parse_dates(self, data):

        """
         This function parses dates into DateTime format

        """

        try:
            for date_column in self.date_column_name:
                data[date_column] = pd.to_datetime(data[date_column], errors='coerce')

        except ParserError as e:
            print(e)
            pass
        except ValueError as e:
            print(e)
            pass

        return data

    def format_date(self, data, date_format):

        """
        Change default DateTime format into desired format. Returns date type as object.
        This type of date will be necessary for visualization. DateTime type will be needed for analysis

        """

        for date_column in self.date_column_name:
            data[f"{date_column}_{date_format}"] = data[date_column].dt.strftime(date_format)

        return data

    def outliers_standard_deviation(self, data):

        """
        This function adds additional boolean columns to help detect outliers.
        If value is an outlier it shows "True" if not - "False".
        Outliers lay outside 3 times standard deviation.
        """

        for column in self.outliers_columns:
            if column not in data.columns:
                continue
            outlier = []
            data_std = np.nanstd(data[column])
            data_mean = np.nanmean(data[column])
            anomaly_cut_off = data_std * 3

            lower_limit = data_mean - anomaly_cut_off
            upper_limit = data_mean + anomaly_cut_off
            print(lower_limit)


            for value in data[column]:
                outlier.append(value > upper_limit or value < lower_limit)

            data[f'STD_DEV_Outlier_{column}'] = outlier

        return data

    def outliers_find_iqr(self, data):

        """
        This function adds additional boolean columns to help detect outliers.
        If value is an outlier it shows "True" if not - "False".
        IQR is the difference between the third quartile and the first quartile (IQR = Q3 -Q1).
        Outliers in this case are defined as the observations that are below (Q1 − 1.5x IQR)
        or boxplot lower whisker or above (Q3 + 1.5x IQR) or boxplot upper whisker.

        """

        for column in self.outliers_columns:
            if column not in data.columns:
                continue
            outlier = []
            q3 = data[column].quantile(.75)
            q1 = data[column].quantile(.25)
            iqr = q3-q1
            outlier_step = 1.5 * iqr


            for value in data[column]:
                outlier.append(value < q1 - outlier_step or value > q3 + outlier_step)

            data[f'IQR_Outlier_{column}'] = outlier
            return data

    def fill_in_outliers_with_nan(self, data, method):

        """
        This function fills in NaN for column's values which are outliers.
        It iterates by columns, locates them and checks if they are outliers.
        Next, it locates columns where values are to be filled in with NaN

        """

        for column in data.columns:
            if f"{method}_Outlier_{column}" in data.columns:
                data.loc[data[f"{method}_Outlier_{column}"] == True, column] = np.nan

        return data

    def outliers_fill_in_interpolation(self, data):

        """
        This function fill in outliers, using method "pad", which is one of basic solutions
        Filling in outliers should be resolved case by case as it highly depends on quality of data.

        """

        data = data.interpolate(method='pad', limit_direction='forward')
        return data

if __name__ == '__main__':
    dl = DataLoader(input_data_path='C:/Users/Ania/Desktop/Nestle Case/X_train_T2.csv',
                    sep=';',
                    decimal=',',
                    coefficient_nulls_removal=50,
                    date_column_name=['date'],
                    outliers_columns=['channel_2','channel_15','channel_17', 'channel_18',
                                   'channel_20','channel_29','channel_43','channel_45',
                                   'channel_52','channel_57','channel_75','channel_89',
                                   'channel_95','channel_101','channel_111'],
                    datetime_duplicated_column_name = ['date_Duplicated'])
    data = dl.get_initial_data()
    data = dl.remove_missing_data(data)
    data = dl.remove_duplicated_columns(data)
    data = dl.delete_duplicated_rows(data)
    data = dl.handle_multi_index(data)
    data = dl.parse_dates(data)
    data = dl.format_date(data, date_format='%d-%m-%Y')
    data = dl.outliers_standard_deviation(data)
    data = dl.outliers_find_iqr(data)
    data = dl.fill_in_outliers_with_nan(data, method='STD_DEV')
    data= dl.outliers_fill_in_interpolation(data)



