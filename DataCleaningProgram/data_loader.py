import pandas as pd
from parser import ParserError
import numpy as np
import matplotlib.pyplot as plt


class DataLoader():
    def __init__(self,
                 input_data_path: str = 'C:/Users/Ania/Desktop/Students1.csv',
                 sep = ';',
                 coefficient_nulls_removal=50,
                 date_column_name=('Date','Old Date'),
                 date_format="%d-%m-%Y",
                 must_have_attr=("year", "month", "day"),
                 datetime_duplicated_column_name=('Date_Duplicated', 'Old Date_Duplicated'),
                 multi_index_columns=('Students', 'Subjects'),
                 date_duplicate_column_name=('Date_Duplicated', 'Old Date_Duplicated'),
                 outliers_columns = ('Age')
                 ):
        self.input_data_path = input_data_path
        self.sep = sep
        self.date_format = date_format
        self.coefficient_nulls_removal = coefficient_nulls_removal
        self.date_column_name = date_column_name
        self.must_have_attr = must_have_attr
        self.datetime_duplicated_column_name = datetime_duplicated_column_name
        self.multi_index_columns = multi_index_columns
        self.date_duplicate_column_name = date_duplicate_column_name
        self.outliers_columns = outliers_columns


    def get_initial_data(self):
        """
         Returns initial data for further clean-up
        """

        index_col = []
        initial_data = pd.read_csv(self.input_data_path, self.sep, index_col=index_col)
        return initial_data

    def remove_missing_data(self, data):
        """
        Checks initial state of nulls. Next, it zips column name with sum of nulls.
        If percentage of sum of nulls is bigger than coefficient_nulls_removal the data is removed.
        E.g. if coefficient_nulls removal is equal to 50, it means that all columns with missing data
        equal or bigger than 50% will be removed
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
        returns list of duplicated columns. When column names are duplicated, DataFrame creates
        duplicate by adding "." and number, e.g. 1. That is why we can distinguish duplicates by eliminating names with "."
        Currently, the code works only when names of columns do not contain dot ".". If more complicated naming occurs,
        this method will be adjusted
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
        removes duplicated columns using list of duplicated columns names
        """
        duplicate_columns_names = self.indicate_duplicated_columns(data)
        data = data.drop(columns=duplicate_columns_names, axis=1)
        return data

    def handle_multi_index(self, data):
        """
        return True if DataFrame is MultiIndex or False if is not
        TODO:zamienić na zwykly indeks dorobić tę część, gdzie okazuje się, że dane są multiindeksowe
        """

        if isinstance(data.index, pd.MultiIndex):
            data = data.reset_index(self.multi_index_columns)
            print(data)
            return data
        else:
            return data


    def delete_duplicated_rows(self, data):
        """
        If data is not MultiIndex delete it automatically
        """

        data = data.drop_duplicates(keep='first')
        return data

    def parse_dates(self, data):
        """
         Parse dates into DateTime format
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

    def format_date(self, data):
        """
        Change default DateTime format into desired format. Returns date type as object.
        This type of date will be necessary for visualization. DateTime type will be needed for analysis

        """

        for date_column in self.date_column_name:
            data[date_column] = data[date_column].dt.strftime(self.date_format)


        print(data)
        return data

    def add_datetime_columns(self, data):
        """
        Add duplicate date columns with datetime format to ease further analyses
        "Format_date" function changes type of date from datetime into string: helpful for visualization, not helpful for analyses
        """
        for (col_name, duplicate_datetime_col) in zip(self.date_column_name, self.datetime_duplicated_column_name):
            data[duplicate_datetime_col] = data.loc[:, col_name]
            data[duplicate_datetime_col] = pd.to_datetime(data[duplicate_datetime_col], errors='coerce')

        print(data)
        data.info()
        return data

    def outliers_standard_deviation(self, data):

        """
        Check for outliers laying outside of 3 times standard deviation. Add to anomalies folder for review
        """

        anomalies = []
        data_std = np.std(data[self.outliers_columns])
        data_mean = np.mean(data[self.outliers_columns])
        anomaly_cut_off = data_std * 3

        lower_limit = data_mean - anomaly_cut_off
        upper_limit = data_mean + anomaly_cut_off
        print(lower_limit)

        for value in data[self.outliers_columns]:
            if value > upper_limit or value < lower_limit:
                anomalies.append(value)

        print(anomalies)
        return anomalies

    def outliers_find_iqr(self, data):

        """
        Selects only data with q1 and q3.
        IQR is the difference between the third quartile and the first quartile (IQR = Q3 -Q1).
        Outliers in this case are defined as the observations that are below (Q1 − 1.5x IQR)
        or boxplot lower whisker or above (Q3 + 1.5x IQR) or boxplot upper whisker
        """

        q3 = data[self.outliers_columns].quantile(.75)
        q1 = data[self.outliers_columns].quantile(.25)
        mask = data[self.outliers_columns].between(q1, q3, inclusive=True)
        print(mask)
        iqr = data.loc[mask, [self.outliers_columns]]
        print(iqr)
        return iqr


if __name__ == '__main__':
    dl = DataLoader()
    data_missing = dl.remove_missing_data(dl.get_initial_data())
    data_duplicate_cols = dl.indicate_duplicated_columns(data_missing )
    data_remove_duplicate_cols = dl.remove_duplicated_columns(data_missing)
    data_remove_duplicate_rows = dl.delete_duplicated_rows(data_remove_duplicate_cols)
    data_flat_multi_index = dl.handle_multi_index(data_remove_duplicate_rows)
    data_parse_dates = dl.parse_dates(data_flat_multi_index)
    data_format_dates = dl.format_date(data_parse_dates)
    data_add_datetime_cols = dl.add_datetime_columns(data_format_dates)
    outliers_std_dev = dl.outliers_standard_deviation(data_parse_dates)
    data_find_iqr = dl.outliers_find_iqr(data_parse_dates)

    #data_clean = dl.remove_ouliers(data.copy()) - odpalanie na kopii dla bezpieczeństwa
