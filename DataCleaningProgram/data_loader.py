from datetime import datetime
from parser import ParserError

import pandas as pd
from tabulate import tabulate
import numpy as np


class DataLoader():
    def __init__(self,
                 input='C:/Users/Ania/Desktop/Nestle Case/X_train_T2.csv',
                 sep=';',
                 coefficient_nulls_removal=10,
                 date_column_name= 1,
                 date_format="%d-%m-%Y"):

        self.input = input
        self.sep = sep
        self.initial_data = pd.read_csv(input, sep)
        self.date_format = date_format
        self.coefficient_nulls_removal = coefficient_nulls_removal
        self.date_column_name = date_column_name

    def get_initial_data(self):
        """
         Returns initial data for further clean-up
        """
        return self.initial_data

    def remove_missing_data(self, data):
        """
        Checks initial state of nulls. Next, it zips column name with sum of nulls.
        If percentage of sum of nulls is bigger than coefficient_nulls_removal the data is removed.
        E.g. if coefficient_nulls removal is equal to 50, it means that all columns with missing data
        equal or bigger than 50% will be removed
        """
        data = data.copy()
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
        """
        data = data.copy()
        non_duplicate_columns_names = set()
        duplicate_columns_names = set()

        index = 0

        for column in data:
            len_before = len(non_duplicate_columns_names)
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
        data = data.copy()
        duplicate_columns_names = self.indicate_duplicated_columns(data)
        data = data.drop(columns=duplicate_columns_names, axis=1)
        return data

    def check_if_multi_index(self, data):
        """
        return True if DataFrame is MutiIndex or False if is not
        """
        data = data.copy()
        if isinstance(data.index, pd.MultiIndex):
            print('Data is MultiIndex')
            return True
        else:
            print('Data is NOT MultiIndex')
            return False

    def delete_duplicated_rows(self, data):
        """
        If data is not MultiIndex delete it automatically
        """
        data = data.copy()
        multi_index = self.check_if_multi_index(data)

        if multi_index == False:
            data = data.drop_duplicates(keep='first')
            print(data)
        else:
            data = data
            print("Check your MultiIndex data")
        # print(tabulate(data, headers='keys'))
        return data

    def parse_dates(self, data):
        """
        Parsing dates into DateTime mode. Version iterating by columns and rows
        """
        for row in range(1, len(data)):
            for self.date_column in range(1, len(data.columns)):
                try:
                    data.iat[row, self.date_column] = datetime.strptime(data.iat[row, self.date_column],
                                                                        self.date_format).date()
                except (ParserError, TypeError, ValueError):
                    pass
        data.info()
        return data

    def parse_dates1(self, data):

        data = data.copy()
        #pd.read_csv('data/data_3.csv', parse_dates=['date']) -pomysł od Natana
        #poczytać o funkcji apply i map - dla obliczenia efektywnego df.apply(np.sqrt),
        # zamiast pierwiastka mogę podać swoją funkcję

        custom_date_parser = lambda x: datetime.strptime(x, self.date_format)
        data = pd.read_csv(data, parse_dates=[self.date_column_name], date_parser=custom_date_parser)
        data.info()
        return data

    # def invoke_parse_data(self, data):
    #     data = data.copy()
    #     data.apply(np.parse_dates())
    #     return data
    #

if __name__ == '__main__':
    dl = DataLoader()
    modified_data_1 = dl.remove_missing_data(dl.get_initial_data())
    modified_data_2 = dl.indicate_duplicated_columns(modified_data_1)
    modified_data_3 = dl.remove_duplicated_columns(modified_data_1)
    modified_data_3 = dl.delete_duplicated_rows(modified_data_3)
    modified_data_4 = dl.parse_dates1(modified_data_3)
    # modified_data_5 = dl.invoke_parse_data(modified_data_4)


