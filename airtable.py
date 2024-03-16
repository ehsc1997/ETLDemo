import requests
import pprint

# OOP approach to Airtable functionality - import class
class AirtableConnector:
    def __init__(self, TOKEN: str, BASE_ID: str):
        self.airtable_base_url = "https://api.airtable.com/v0"
        self.TOKEN = TOKEN
        self.BASE_ID = BASE_ID


    def _generate_schema(self, columns: list[str], dtypes: list[str]):
        if len(columns) != len(dtypes):
            raise ValueError('Los argumentos "columns" y "dtypes" necesitan ser del mismo tamaño')

        fields = zip(columns, dtypes)
        schema = [{ 
                    "name": column,
                    "type": dtype
                }
                for column, dtype in fields
            ]
        
        return schema


    def create_table(self, table_name: str, columns: list[str], dtypes: list[str], description="") -> requests.Response:
        # URL
        endpoint = f"{self.airtable_base_url}/meta/bases/{self.BASE_ID}/tables"

        # Headers
        headers = {
            "Authorization" : f"Bearer {self.TOKEN}",
            "Content-Type"  : "application/json"
        }
        
        schema = self._generate_schema(columns, dtypes)

        # Put schema, description and table name into data
        data = {
            "description": description,
            "fields": schema,
            "name": table_name,
        }
        response = requests.post(url = endpoint, json = data, headers = headers)
        
        error = response.json().get('error')
        if error:
            raise ValueError(f'{error["type"]}: {error["message"]}')

        return response
    

    def load_from_df(self, df, TABLE_ID):
        # URL
        endpoint = f"{self.airtable_base_url}/{self.BASE_ID}/{TABLE_ID}"
        
        # Headers
        headers = {"Authorization" : f"Bearer {self.TOKEN}",
                "Content-Type"  : "application/json"}
        
        # Store response objects
        responses = []

        # Add 10 rows at a time
        for i in range(0, df.shape[0], 10):

            try:
                samples = [{"fields" : df.iloc[i+j, :].to_dict()} for j in range(10)]
            except IndexError:
                continue
            finally:
                datos_subir = {"records" : samples,
                            "typecast" : True}
                
                response = requests.post(url = endpoint, json = datos_subir, headers = headers)

            print(f"response: {response.status_code}")

            print(f"endpoint: {response.url}")

            print("-"*120)

            pprint(response.json(), sort_dicts = False)

            print("-"*120)

            responses.append(response)

        return responses
        
# Functional approach - helper functions can also be imported
def format_airtable_schema(names, dtypes):

    if len(names) != len(dtypes):
        raise ValueError('Los dos argumentos necesitan ser del mismo tamaño')

    fields = zip(names, dtypes)
    schema = [{"name": name,
               "type": dtype}
               for name, dtype in fields
               ]

    return schema


def airtable_create(TOKEN, BASE_ID, table_name, schema, description):
    # URL
    airtable_base_url = "https://api.airtable.com/v0"
    endpoint = f"{airtable_base_url}/meta/bases/{BASE_ID}/tables"

    # Headers
    headers = {"Authorization" : f"Bearer {TOKEN}",
            "Content-Type"  : "application/json"}
    
    # Put schema, description and table name into data
    data = {
        "description": description,
        "fields": schema,
        "name": table_name,
    }

    response = requests.post(url = endpoint, json = data, headers = headers)

    return response
    

def airtable_load(df, TOKEN, BASE_ID, TABLE_ID):
    # URL
    airtable_base_url = "https://api.airtable.com/v0"
    endpoint = f"{airtable_base_url}/{BASE_ID}/{TABLE_ID}"
    
    # Headers
    headers = {"Authorization" : f"Bearer {TOKEN}",
            "Content-Type"  : "application/json"}
    
    # Store response objects
    responses = []

    # Add 10 rows at a time
    for i in range(0, df.shape[0], 10):

        try:
            samples = [{"fields" : df.iloc[i+j, :].to_dict()} for j in range(10)]
        except IndexError:
            continue
        finally:
            datos_subir = {"records" : samples,
                        "typecast" : True}
            
            response = requests.post(url = endpoint, json = datos_subir, headers = headers)

        print(f"response: {response.status_code}")

        print(f"endpoint: {response.url}")

        print("-"*120)

        pprint(response.json(), sort_dicts = False)

        print("-"*120)

        responses.append(response)

    return responses