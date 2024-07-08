import requests
import pandas as pd


class IngestAPI:
    
    def __init__(self, spark_session):
        self.spark = spark_session


    def fetch_api_data(self, url):
        """Baixa dados da API de testes e retorna em formato de dataframe spark"""
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            p_df = pd.DataFrame(data, index=data.keys())
            df = self.spark.createDataFrame(p_df)
            return df
        else:
            response.raise_for_status()

