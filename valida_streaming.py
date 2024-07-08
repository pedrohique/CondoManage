from pyspark.sql import SparkSession

# Inicializa a sessão Spark
spark = SparkSession.builder \
    .appName("Leitura de Dados Processados") \
    .getOrCreate()

def get_schema(path):
    return spark.read.parquet(path).schema

def read_and_show_data():
    path_transacoes_por_condominio = '/home/pedro/Projetos/CondoManage/s3_simulator/processed/transacoes_por_condominio'
    path_valor_transacoes_por_morador = '/home/pedro/Projetos/CondoManage/s3_simulator/processed/valor_transacoes_por_morador'
    path_transacoes_diarias_por_tipo = '/home/pedro/Projetos/CondoManage/s3_simulator/processed/transacoes_diarias_por_tipo'

    # Obtém os esquemas dos arquivos Parquet
    schema_transacoes_por_condominio = get_schema(path_transacoes_por_condominio)
    schema_valor_transacoes_por_morador = get_schema(path_valor_transacoes_por_morador)
    schema_transacoes_diarias_por_tipo = get_schema(path_transacoes_diarias_por_tipo)

    # Lê os dados em streaming
    condominios_df = spark.readStream.schema(schema_transacoes_por_condominio).parquet(path_transacoes_por_condominio)
    moradores_df = spark.readStream.schema(schema_valor_transacoes_por_morador).parquet(path_valor_transacoes_por_morador)
    imoveis_df = spark.readStream.schema(schema_transacoes_diarias_por_tipo).parquet(path_transacoes_diarias_por_tipo)

    # Inicia as consultas de leitura em streaming
    query_condominios = condominios_df.writeStream.format("console").start()
    query_moradores = moradores_df.writeStream.format("console").start()
    query_imoveis = imoveis_df.writeStream.format("console").start()

    # Aguarda a conclusão das consultas
    query_condominios.awaitTermination()
    query_moradores.awaitTermination()
    query_imoveis.awaitTermination()
    spark.stop()

if __name__ == '__main__':
    read_and_show_data()
