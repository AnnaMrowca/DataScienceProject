import pandas as pd


def read_dataset():
    dataset = 'C:/Users/Ania/Desktop/Nestle Case/X_train_T2.csv'
    df = pd.read_csv(dataset, sep=';', header=0, parse_dates=[1], decimal=',')
    return(df)

print(read_dataset())


def remove_duplicates():
    df = pd.DataFrame(read_dataset())
    df.sort_values(by='key')
    df.drop_duplicates(subset='key', keep='first', inplace=False)
print(remove_duplicates())



