from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DecimalType
from faker import Faker
import datetime
from decimal import Decimal

class CreateDataFake:
    """CRIA DADOS FAKE PARA SUBSTITUIR O BANCO DE DADOS"""
    def __init__(self):
        self.spark = SparkSession.builder.appName('CondominioAleatoryData').getOrCreate()
        self.fake = Faker()
        self.fake.seed_instance(42)
        self.data_size = 1000
        self.start_date = datetime.date(2024, 1, 1)
        self.end_date = datetime.date(2024, 7, 31)


    def create_data_pipeline(self, table):
        table_dict = {
                'condominios': self.create_condominios(),
                'moradores': self.create_moradores(),
                'imoveis': self.create_imoveis(),
                'transacoes':self.create_transacoes()
                }
        return table_dict.get(table, 'tabela não encontrada')

    def create_condominios(self):
        condominios_data = [
            (i + 1, self.fake.company(), self.fake.address()) for i in range(self.data_size )
        ]
        condominios_schema = StructType([
            StructField('condominio_id', IntegerType(), False),
            StructField('nome', StringType(), False),
            StructField('endereco', StringType(), False)
        ])

        condominios_df = self.spark.createDataFrame(condominios_data, schema=condominios_schema)

        return condominios_df


    def create_moradores(self):

        moradores_data = [
            (i + 1, self.fake.name(), self.fake.random.randint(1, self.data_size), self.fake.date_between(start_date=self.start_date, end_date=self.end_date))
            for i in range(self.data_size )
        ]
        moradores_schema = StructType([
            StructField('morador_id', IntegerType(), False),
            StructField('nome', StringType(), False),
            StructField('condominio_id', IntegerType(), False),
            StructField('data_registro', DateType(), False)
        ])

        moradores_df = self.spark.createDataFrame(moradores_data, schema=moradores_schema)

        return moradores_df


    def create_imoveis(self):
        imoveis_data = [
            (i + 1, self.fake.random.choice(['Apartamento', 'Casa']), self.fake.random.randint(1, self.data_size ), Decimal(self.fake.random.uniform(200000, 1000000)).quantize(Decimal('0.01')))
            for i in range(self.data_size )
        ]
        imoveis_schema = StructType([
            StructField('imovel_id', IntegerType(), False),
            StructField('tipo', StringType(), False),
            StructField('condominio_id', IntegerType(), False),
            StructField('valor', DecimalType(10 , 2), False)
        ])

        imoveis_df = self.spark.createDataFrame(imoveis_data, schema=imoveis_schema)
        return imoveis_df

    def create_transacoes(self):
        transacoes_data = [
            (i + 1, self.fake.random.randint(1, self.data_size ), self.fake.random.randint(1, self.data_size ), self.fake.date_between(start_date=self.start_date, end_date=self.end_date), Decimal(self.fake.random.uniform(100000, 500000)).quantize(Decimal('0.01')))
            for i in range(self.data_size )
        ]
        transacoes_schema = StructType([
            StructField('transacao_id', IntegerType(), False),
            StructField('imovel_id', IntegerType(), False),
            StructField('morador_id', IntegerType(), False),
            StructField('data_transacao', DateType(), False),
            StructField('valor_transacao', DecimalType(10 , 2), False)
        ])


        transacoes_df = self.spark.createDataFrame(transacoes_data, schema=transacoes_schema)
        return transacoes_df


