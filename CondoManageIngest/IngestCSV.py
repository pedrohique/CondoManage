import glob
import csv

class IngCSV:
    """A classe necessita da implementação de um metodos de validações 
    antes de realizar o merge dos dataframes, necessario tambem implementar 
    uma verificação se o arquivo ja foi lido para não acabar duplicando os dados no datalake"""

    def __init__(self, spark_session):

        self.spark = spark_session


    def read_csv(self, file_path):
        """Baixa dados dos CSVs de testes e retorna em formato de dataframe spark"""
        df_base = None
        for file in glob.glob(f"{file_path}/*.csv"):
            if not df_base:
                df_base = self.spark.read.csv(file, header=True, inferSchema=True, sep=';')
            else:
                df_new = self.spark.read.csv(file, header=True, inferSchema=True, sep=';')
                df_base = self.union_df(df_base, df_new)
        
        return df_base


    def union_df(self, df_base, df_new):
        df = df_base.union(df_new)
        return df