################################################################################
#
#
#
#       acquire.py
#
#       Description: description
#
#       Variables:
#
#           datasets
#
#       Functions:
#
#           get_data(url, endpoint, name, show_output)
#           get_items(show_output = True)
#           get_stores(show_output = True)
#           get_sales(show_output = True)
#           load_data(name, use_cache = True, cache_data = True, show_output = True)
#           combine_data(sales, items, stores)
#           get_open_power_systems_data()
#
#
################################################################################

import os
import requests
import pandas as pd

################################################################################

def get_data(url: str, endpoint: str, name: str, show_output: bool = True) -> pd.DataFrame:
    '''
        Return a dataframe containing the data from the Codeup API stored at 
        endpoint.
    
        Parameters
        ----------
        url: str
            The Codeup API URL without the endpoint trailing.

        endpoint: str
            The endpoint from which to retrieve the data.

        name: str
            The name of the table to download.

        show_output: bool, default True
            If True output regarding the state of the data loading will be 
            printed to the console.
    
        Returns
        -------
        DataFrame: A pandas dataframe containing the data from the Codeup API.
    '''

    data = pd.DataFrame()
    
    while True:
        if show_output: print(f'Reading page {endpoint}', end = '\r')

        contents = requests.get(url + endpoint).json()
        page_contents = pd.DataFrame(contents['payload'][name])
        data = pd.concat([data, page_contents])
        
        if not (next_page := contents['payload']['next_page']):
            break
            
        endpoint = next_page
        
    data = data.reset_index().drop(columns = 'index')
    if show_output: print('Loading complete. Returning data.')
        
    return data

################################################################################

def get_items(show_output: bool = True) -> pd.DataFrame:
    '''
        Returns the items data from the Codeup API.
    
        Parameters
        ----------
        show_output: bool, default True
            If True output regarding the state of the data loading will be 
            printed to the console.
    
        Returns
        -------
        DataFrame: A pandas dataframe containing the items data.
    '''

    data = get_data('https://api.data.codeup.com', '/api/v1/items', 'items', show_output)
    return data

################################################################################

def get_stores(show_output: bool = True) -> pd.DataFrame:
    '''
        Returns the stores data from the Codeup API.
    
        Parameters
        ----------
        show_output: bool, default True
            If True output regarding the state of the data loading will be 
            printed to the console.
    
        Returns
        -------
        DataFrame: A pandas dataframe containing the stores data.
    '''

    data = get_data('https://api.data.codeup.com', '/api/v1/stores', 'stores', show_output)
    return data

################################################################################

def get_sales(show_output: bool = True) -> pd.DataFrame:
    '''
        Returns the sales data from the Codeup API.
    
        Parameters
        ----------
        show_output: bool, default True
            If True output regarding the state of the data loading will be 
            printed to the console.
    
        Returns
        -------
        DataFrame: A pandas dataframe containing the sales data.
    '''

    data = get_data('https://api.data.codeup.com', '/api/v1/sales', 'sales', show_output)
    return data

################################################################################

datasets = {
    'items' : {
        'file' : 'items.csv',
        'function' : get_items
    },
    'stores' : {
        'file' : 'stores.csv',
        'function' : get_stores
    },
    'sales' : {
        'file' : 'sales.csv',
        'functions' : get_sales
    }
}

################################################################################

def load_data(name: str, use_cache: bool = True, cache_data: bool = True, show_output: bool = True) -> pd.DataFrame:
        '''
            Return a dataframe containing data for name.

            If a .csv file containing the data does not already exist the data 
            will be cached in a .csv file inside the current working directory. 
            Otherwise, the data will be read from the .csv file.

            Parameters
            ----------
            name: str
                The name of the dataset to be loaded.

            use_cache: bool, default True
                If True the dataset will be retrieved from a csv file if one
                exists, otherwise, it will be retrieved from the MySQL database. 
                If False the dataset will be retrieved from the MySQL database
                even if the csv file exists.

            cache_data: bool, default True
                If True the dataset will be cached in a csv file.

            show_output: bool, default True
                If True output regarding the state of the data loading will be 
                printed to the console.

            Returns
            -------
            DataFrame: A Pandas DataFrame containing data from the source provided.
        '''

        # If the file is cached, read from the .csv file
        if os.path.exists(datasets[name]['file']) and use_cache:
            if show_output: print('Reading from .csv file.')
            return pd.read_csv(datasets[name]['file'])
        
        # Otherwise read from the mysql database
        else:
            if show_output: print('Reading from API.')
            df = datasets[name]['function'](show_output)

            # Cache the data in a .csv file, if that is what we want
            if cache_data:
                df.to_csv(datasets[name]['file'], index = False)

            return df

################################################################################

def combine_data(sales: pd.DataFrame, items: pd.DataFrame, stores: pd.DataFrame) -> pd.DataFrame:
    '''
        Join the sales, items, and stores dataframes into one dataframe.
    
        Parameters
        ----------
        sales: DataFrame
            A dataframe containing the sales data from the Codeup API.

        items: DataFrame
            A dataframe containing the items data from the Codeup API.

        stores: DataFrame
            A dataframe containing the stores data from the Codeup API.
    
        Returns
        -------
        DataFrame: A pandas dataframe containing the sales, items, and stores
            data in one dataframe.
    '''

    df = sales.merge(items, how = 'inner', left_on = 'item', right_on = 'item_id')
    df = df.drop(columns = 'item_id')
    df = df.merge(stores, how = 'inner', left_on = 'store', right_on = 'store_id')
    df = df.drop(columns = 'store_id')
    return df

################################################################################

def get_open_power_systems_data() -> pd.DataFrame:
    '''
        Load the Open Power Systems Data for Germany from the online csv file.
    
        Returns
        -------
        DataFrame: A pandas dataframe containing the open power systems data.
    '''

    return pd.read_csv('https://raw.githubusercontent.com/jenfly/opsd/master/opsd_germany_daily.csv')