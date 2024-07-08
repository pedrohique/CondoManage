from CondoManageIngest.create_fake_dataset import CreateDataFake

class IngestDB:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.tables = ('transacoes', 'imoveis', 'moradores', 'condominios')
        self.host = 'host.condomanage.com'
        self.database = 'CondoDb'
        self.user = 'condo_manage'
        self.password = 'Condo_Manage_123@'
        self.data_dict = {}
        self.fake_data = CreateDataFake()

    def read_postgresql(self, query):
        """Recebe uma query, faz a conexão com banco de dados e retorna um Dataframe Spark"""
        url = f"jdbc:postgresql://{self.host}/{self.database}"
        properties = {
            "user": self.user,
            "password": self.password,
            "driver": "org.postgresql.Driver"
        }
        df = self.spark.read.jdbc(url=url, table=f"({query}) as query", properties=properties)
        return df


    def get_data(self):
        """
        Teoricamente deveria se conectar ao banco de dados postgresql retornar em formato de dataframe spark
        Porem tomei a liberdade de gerar uma classe que gera dados ficticios e os retorna em formato de dataframe spark
        """
        for table in self.tables:
            data = None
            # query = 'SELECT * FROM {}'.format(table)  
            # data = self.read_postgresql() 

            # como não possuo um banco criado
            data = self.fake_data.create_data_pipeline(table)

            if data:
                self.data_dict[table] = data
        return self.data_dict
    

