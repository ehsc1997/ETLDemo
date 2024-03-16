import configparser
from airtable import AirtableConnector

def main():
    # Read in configuration
    config = configparser.ConfigParser()
    config.read('config.ini')

    # Extract config for AirtableConnector initialisation
    TOKEN = config.get('airtable', 'TOKEN')
    BASE_ID = config.get('airtable', 'BASE_ID')

    connector = AirtableConnector(TOKEN, BASE_ID)

    response = connector.create_table('test3', ['column1', 'column2', 'column3'], ['singleLineText', 'singleLineText', 'singleLineText'])

    TABLE_ID = response.json().get('id')

    if not TABLE_ID:
        print(response.json())

    print(TABLE_ID)


if __name__ == "__main__":
    main()