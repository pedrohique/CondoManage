import logging
from pyspark.sql import SparkSession
from CondoManageIngest import IngestAPI, IngestDB, IngestCSV

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ingestDataLogger")

file_handler = logging.FileHandler('ingest_log.log')
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

class DataIngest:
    def __init__(self, spark):
        self.spark = spark
        self.api_obj = IngestAPI.IngestAPI(self.spark)
        self.db_obj = IngestDB.IngestDB(self.spark)
        self.csv_obj = IngestCSV.IngCSV(self.spark)
        self.api_data = None
        self.db_data = None
        self.csv_data = None
        logger.info("Inicializando DataIngest")

    def ingest_data_pipeline(self):        
        """Coordena a ingestão chamando os métodos necessários para baixar e inserir no S3"""
        logger.info("Iniciando o pipeline de ingestão de dados")
        try:
            self.api_data = self.api_obj.fetch_api_data('https://jsonplaceholder.typicode.com/todos/1')
            logger.info("Dados da API ingeridos")
        except Exception as e:
            logger.error(f"Erro ao ingerir dados da API: {e}")

        try:
            self.csv_data = self.csv_obj.read_csv('csv_files')
            logger.info("Dados CSV ingeridos")
        except Exception as e:
            logger.error(f"Erro ao ingerir dados CSV: {e}")

        try:
            self.db_data = self.db_obj.get_data()
            logger.info("Dados do banco de dados ingeridos")
        except Exception as e:
            logger.error(f"Erro ao ingerir dados do banco de dados: {e}")

        self.input_raw_data()

    def input_raw_data(self):
        """Envia os dados ao S3 fake (pasta criada no diretório)"""
        path = 's3_simulator/raw/'  # s3://condomanage-data-lake/raw/
        logger.info("Enviando dados para o S3 fake")

        if self.api_data:
            api_data_path = path + 'api_data'
            try:
                if self.file_exists(api_data_path):
                    self.api_data.write.mode('append').parquet(api_data_path)
                    logger.info("Dados da API enviados em modo append")
                else:
                    self.api_data.write.parquet(api_data_path)
                    logger.info("Dados da API enviados")
            except Exception as e:
                logger.error(f"Erro ao enviar dados da API para o S3 fake: {e}")

        if self.csv_data:
            csv_data_path = path + 'csv_data'
            try:
                if self.file_exists(csv_data_path):
                    self.csv_data.write.mode('append').parquet(csv_data_path)
                    logger.info("Dados CSV enviados em modo append")
                else:
                    self.csv_data.write.parquet(csv_data_path)
                    logger.info("Dados CSV enviados")
            except Exception as e:
                logger.error(f"Erro ao enviar dados CSV para o S3 fake: {e}")

        if self.db_data:
            for table in self.db_data.keys():
                db_data_path = path + f'db_data/{table}'
                try:
                    if self.db_data.get(table):
                        if self.file_exists(db_data_path):
                            self.db_data.get(table).write.mode('append').parquet(db_data_path)
                            logger.info(f"Dados da tabela {table} enviados em modo append")
                        else:
                            self.db_data.get(table).write.parquet(db_data_path)
                            logger.info(f"Dados da tabela {table} enviados")
                except Exception as e:
                    logger.error(f"Erro ao enviar dados da tabela {table} para o S3 fake: {e}")

    def file_exists(self, path):
        fs = self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(self.spark._jsc.hadoopConfiguration())
        exists = fs.exists(self.spark._jvm.org.apache.hadoop.fs.Path(path))
        return exists

if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("MultiSourceData") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.2.23") \
        .getOrCreate()
    data_ingest = DataIngest(spark)
    data_ingest.ingest_data_pipeline()
    spark.stop()
