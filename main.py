import configparser
import pandas as pd
import numpy as np
from airtable import AirtableConnector

def main():
    # Read in configuration
    config = configparser.ConfigParser()
    config.read('config.ini')

    # Extract config for AirtableConnector initialisation
    TOKEN = config.get('airtable', 'TOKEN')
    BASE_ID = config.get('airtable', 'BASE_ID')

    connector = AirtableConnector(TOKEN, BASE_ID)

    # df = pd.read_csv("todas_recetas.csv")
    # df = df.replace(np.nan, None)[['nombres','descripciones','comensales','tiempos','dificultades']]
    # columns = list(df.columns)
    # dtypes = ['singleLineText']*len(df.columns)
    # response = connector.create_table('ensaladas', columns, dtypes)

    # TABLE_ID = response.json().get('id')

    # if not TABLE_ID:
    #     print(response.json())
    
    # responses = connector.load_from_df(df, TABLE_ID)

    # return responses
    TABLE_ID = 'tblgwN6hj9cXbks65'

    print(connector.to_df(TABLE_ID))


if __name__ == "__main__":
    responses = main()