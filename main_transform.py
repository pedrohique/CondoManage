from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, window
from pyspark.sql.types import TimestampType
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ProcessDataLogger")

file_handler = logging.FileHandler('process_data.log')
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

class ProcessData:
    def __init__(self):
        logger.info("Iniciando a sessão Spark")
        self.spark = SparkSession.builder \
            .appName("CondoManage Data Processing") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.postgresql:postgresql:42.2.23") \
            .getOrCreate()
        self.spark.sparkContext.setCheckpointDir("/tmp/checkpoint")
        
        self.condominios_df = None
        self.moradores_df = None
        self.imoveis_df = None
        self.transacoes_df = None

    def process_data_pipeline(self):
        logger.info("Iniciando o pipeline de processamento de dados")
        self.read_data()
        self.process_data()
        self.input_processed_data()
        self.spark.stop()
        
    def read_data(self):
        logger.info("Lendo dados de entrada")
        path = 's3_simulator/raw/db_data/'

        schema_condominios = self.get_schema(path + "condominios")
        schema_moradores = self.get_schema(path + "moradores")
        schema_imoveis = self.get_schema(path + "imoveis")
        schema_transacoes = self.get_schema(path + "transacoes")

        self.condominios_df = self.spark.readStream.schema(schema_condominios).parquet(path + "condominios")
        self.moradores_df = self.spark.readStream.schema(schema_moradores).parquet(path + "moradores")
        self.imoveis_df = self.spark.readStream.schema(schema_imoveis).parquet(path + "imoveis")
        self.transacoes_df = self.spark.readStream.schema(schema_transacoes).parquet(path + "transacoes") \
            .withColumn("data_transacao", col("data_transacao").cast(TimestampType()))

    def get_schema(self, path):
        logger.info(f"Obtendo schema para {path}")
        return self.spark.read.parquet(path).schema

    def process_data(self):
        logger.info("Processando dados")
        watermark_duration = "1 hour"
        if self.transacoes_df:
            # Calcula total de transações por condomínio
            if self.imoveis_df:
                self.transacoes_por_condominio = self.transacoes_df \
                    .withWatermark("data_transacao", watermark_duration) \
                    .join(self.imoveis_df, 'imovel_id') \
                    .groupBy('condominio_id', window('data_transacao', watermark_duration)) \
                    .agg(count('transacao_id').alias('total_transacoes'))

            # Calcula o valor total das transações por morador
            self.valor_transacoes_por_morador = self.transacoes_df \
                .withWatermark("data_transacao", watermark_duration) \
                .groupBy('morador_id', window('data_transacao', watermark_duration)) \
                .agg(sum('valor_transacao').alias('valor_total_transacoes'))

            # Agrega as transações diárias por tipo de imóvel
            if self.moradores_df:
                self.transacoes_diarias_por_tipo = self.transacoes_df \
                    .withWatermark("data_transacao", watermark_duration) \
                    .join(self.imoveis_df, 'imovel_id') \
                    .groupBy(window("data_transacao", "1 day"), 'tipo') \
                    .agg(count('transacao_id').alias('total_transacoes'))

    def input_processed_data(self):
        logger.info("Salvando dados transformados no Data Lake")
        path = 's3_simulator/processed/'
        try:
            query1 = self.transacoes_por_condominio.writeStream \
                .outputMode('append') \
                .format('parquet') \
                .option('path', path + "transacoes_por_condominio/") \
                .option('checkpointLocation', path + "checkpoints/transacoes_por_condominio/") \
                .start()

            query2 = self.valor_transacoes_por_morador.writeStream \
                .outputMode('append') \
                .format('parquet') \
                .option('path', path + "valor_transacoes_por_morador/") \
                .option('checkpointLocation', path + "checkpoints/valor_transacoes_por_morador/") \
                .start()

            query3 = self.transacoes_diarias_por_tipo.writeStream \
                .outputMode('append') \
                .format('parquet') \
                .option('path', path + "transacoes_diarias_por_tipo/") \
                .option('checkpointLocation', path + "checkpoints/transacoes_diarias_por_tipo/") \
                .start()

            query1.awaitTermination()
            query2.awaitTermination()
            query3.awaitTermination()

        except Exception as e:
            logger.error(f"Erro ao salvar dados transformados: {str(e)}")

if __name__ == '__main__':
    logger.info("Inicializando o objeto de processamento de dados")
    process_obj = ProcessData()
    process_obj.process_data_pipeline()
    logger.info("Pipeline de processamento de dados concluído")
