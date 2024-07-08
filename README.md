CondoManage
Descrição

O CondoManage é um sistema de gerenciamento de condomínios desenvolvido em Python. Ele permite a ingestão, transformação e validação de dados relacionados ao condomínio.

Instruções de Uso

Instalação de Dependências: 
Certifique-se de ter o Python instalado e, em seguida, instale as dependências do projeto executando o seguinte comando:

    pip install -r requirements.txt

Ingestão de Dados: 
Execute o seguinte comando para realizar a ingestão inicial dos dados:

    spark-submit main_ingest.py

Processamento de Streaming: 
Inicie o processamento de streaming de dados com o seguinte comando:

    spark-submit --master local[*] --packages org.postgresql:postgresql:42.2.23 main_transform.py

Validação dos Dados Inseridos: 
Verifique a integridade dos dados inseridos usando o seguinte comando:

    python3 valida_streaming.py

