'''

    prepare.py

    Description: Preparation functions for the data preparation exercises for
        the time series analysis module.

    Variables:

        None

    Functions:

    set_date_to_index(df)
    engineer_new_features(df)
    prepare_data(df)
    plot_distributions(df)
    ops_set_date_to_index(df)
    ops_engineer_new_features(df)
    ops_handle_missing_value(df)
    prepare_ops_data(df)

'''

################################################################################

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

################################################################################

def set_date_to_index(df: pd.DataFrame) -> pd.DataFrame:
    '''
        Sets the date column of the store sales data to a datetime type and 
        sets the date column as the index.
    
        Parameters
        ----------
        df: DataFrame
            The store sales dataset.
    
        Returns
        -------
        DataFrame: The store sales data with the date column set as the index.
    '''

    df = df.copy()
    df.sale_date = df.sale_date.apply(lambda date: date[ : -13])
    df.sale_date = pd.to_datetime(df.sale_date, format='%a, %d %b %Y')
    df = df.set_index('sale_date').sort_index()
    return df

################################################################################

def engineer_new_features(df: pd.DataFrame) -> pd.DataFrame:
    '''
        Creates month, weekday, and sales_total columns for the store sales 
        data.
    
        Parameters
        ----------
        df: DataFrame
            The store sales dataset.
    
        Returns
        -------
        DataFrame: The store sales data with month, weekday, and sales_total
            columns added.
    '''

    df = df.copy()
    df['month'] = df.index.strftime('%m-%b')
    df['weekday'] = df.index.strftime('%w-%a')
    df['sales_total'] = df.sale_amount * df.item_price
    return df

################################################################################

def prepare_data(df: pd.DataFrame) -> pd.DataFrame:
    '''
        Prepares the store sales data.
    
        Parameters
        ----------
        df: DataFrame
            The original store sales data.
    
        Returns
        -------
        DataFrame: The prepared store sales data.
    '''

    df = set_date_to_index(df)
    df = engineer_new_features(df)
    return df

################################################################################

def plot_distributions(df: pd.DataFrame) -> None:
    '''
        Plots histograms of each column in df.
    
        Parameters
        ----------
        df: DataFrame
            A pandas dataframe containing data.
    '''

    columns = df.columns

    fig, ax = plt.subplots(nrows = len(columns), ncols = 1, figsize = (10, 3 * len(columns)))

    for index, column in enumerate(columns):
        sns.histplot(df[column], bins = 20, ax = ax[index])
        ax[index].set(title = column)

    plt.tight_layout()
    plt.show()

################################################################################

def ops_set_date_to_index(df: pd.DataFrame) -> pd.DataFrame:
    '''
        Sets the date columns to a datetime type and sets the date column as 
        the index for the OPS data.
    
        Parameters
        ----------
        df: DataFrame
            The OPS dataset.
    
        Returns
        -------
        DataFrame: The OPS data with the date column set as the index.
    '''

    df = df.copy()
    df.date = pd.to_datetime(df.date)
    df = df.set_index('date').sort_index()
    return df

################################################################################

def ops_engineer_new_features(df: pd.DataFrame) -> pd.DataFrame:
    '''
        Creates month and year columns for the OPS data.
    
        Parameters
        ----------
        df: DataFrame
            The OPS dataset.
    
        Returns
        -------
        DataFrame: The OPS data with month and year columns added.
    '''

    df = df.copy()
    df['month'] = df.index.strftime('%m-%b')
    df['year'] = df.index.year
    return df

################################################################################

def ops_handle_missing_value(df: pd.DataFrame) -> pd.DataFrame:
    '''
        Handle missing values in the OPS data. Fills all NaNs with 0 and 
        re-engineers the wind_solar column to be the sum of the wind and 
        solar columns.
    
        Parameters
        ----------
        df: DataFrame
            The OPS dataset.
    
        Returns
        -------
        DataFrame: The OPS data with missing values handled.
    '''

    df = df.copy()
    df = df.fillna(0)
    df.wind_solar = df.wind + df.solar
    return df

################################################################################

def prepare_ops_data(df: pd.DataFrame) -> pd.DataFrame:
    '''
        Prepare the open power systems data.
    
        Parameters
        ----------
        df: DataFrame
            The original OPS data.
    
        Returns
        -------
        DataFrame: The prepared OPS data.
    '''

    df.columns = [col.replace('+', '_').lower() for col in df.columns]
    df = ops_set_date_to_index(df)
    df = ops_engineer_new_features(df)
    df = ops_handle_missing_value(df)
    return df