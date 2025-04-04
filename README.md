# Criando uma Solução Completa com AWS Glue para Processamento de Arquivos Posicionais no S3

## Introdução
Empresas frequentemente precisam processar grandes volumes de dados armazenados na nuvem. Quando esses dados chegam de forma parcelada ao longo do dia, torna-se essencial uma solução automatizada para consolidá-los, transformá-los e processá-los de maneira eficiente. Neste artigo, vamos detalhar uma solução usando **AWS Glue**, **AWS Lambda**, **Amazon S3** e **CloudWatch** para processar **arquivos posicionais** de forma escalável e monitorada.

## Objetivo
Criar um fluxo de processamento que:
1. **Receba diariamente 7 arquivos posicionais** no S3.
2. **Dispare automaticamente uma função Lambda sempre que um novo arquivo chegar no S3**.
3. **Converta cada arquivo recebido em Parquet**.
4. **Verifique se todos os arquivos esperados chegaram** e, quando isso ocorrer, inicie o Glue Job.
5. **O Glue Job lê os arquivos Parquet, aplica filtros e junções, gera um novo Parquet e salva os dados no OpenSearch**.
6. **Monitore o processamento no CloudWatch**, registrando erros e garantindo que o Glue Job tenha sido concluído com sucesso até às 12h do dia.

## Arquitetura da Solução
A solução será composta pelos seguintes componentes AWS:
- **Amazon S3**: Armazena os arquivos posicionais brutos e os arquivos convertidos em Parquet.
- **AWS Lambda**: Executa a conversão dos arquivos para Parquet e verifica se todos os arquivos esperados chegaram.
- **AWS Glue**: Processa os arquivos Parquet, aplica transformações e salva os dados na fonte de destino.
- **Amazon OpenSearch**: Índice de destino para os dados processados.
- **Amazon CloudWatch**: Monitora erros no processamento e verifica se o Glue Job foi concluído com sucesso antes das 12h.
- **Amazon EventBridge**: Agenda a verificação do status do Glue Job ao meio-dia.
- **AWS S3 Event Notification**: Dispara a função Lambda sempre que um novo arquivo é carregado no S3.

## Fluxo de Processamento
1. **Arquivos posicionais chegam ao S3** em horários distintos.
2. **O Amazon S3 dispara automaticamente uma função Lambda** para converter o arquivo em Parquet e armazená-lo em uma subpasta no S3.
3. **A Lambda verifica se todos os 7 arquivos do dia já chegaram**.
4. **Se todos os arquivos do dia estiverem disponíveis, a Lambda dispara o Glue Job**.
5. **O Glue Job lê os arquivos Parquet, aplica filtros e junções, gera um novo Parquet e salva os dados no OpenSearch**.
6. **Caso ocorra um erro no processamento, um alerta é enviado ao CloudWatch**.
7. **Ao meio-dia, um processo verifica se o Glue Job foi concluído com sucesso** e emite um alerta caso ele ainda esteja rodando ou nem tenha sido executado.

## Implementação Passo a Passo

### 1. Criando o Bucket no Amazon S3
- Criar um bucket S3 chamado `meu-bucket-dados`.
- Criar duas subpastas:
  - `/dados-posicionais/` → Para armazenar os arquivos brutos.
  - `/dados-parquet/` → Para armazenar os arquivos convertidos.

### 2. Configurando o Amazon S3 para Disparar a AWS Lambda
Para que a AWS Lambda seja acionada automaticamente quando um novo arquivo for carregado no bucket S3, configuramos um **S3 Event Notification**:
1. Acesse o **Amazon S3** no console da AWS.
2. Navegue até o bucket `meu-bucket-dados`.
3. Vá para a aba **Properties** e encontre **Event notifications**.
4. Clique em **Create event notification**.
5. Nomeie o evento como `DispararLambdaAoReceberArquivo`.
6. Configure a opção **Event types** para **PUT (All object create events)**.
7. Defina o prefixo `/dados-posicionais/` para garantir que apenas novos arquivos nessa pasta disparem a Lambda.
8. Escolha **Trigger Lambda function** e selecione a função Lambda que criaremos a seguir.

### 3. Criando a AWS Lambda para Converter Arquivos para Parquet
A função Lambda será acionada automaticamente ao receber um novo arquivo no S3. Ela lê o arquivo posicional, extrai os campos conforme a estrutura definida e salva o resultado no formato Parquet.

```python
import boto3
import pyarrow.parquet as pq
import pyarrow as pa
import io
import os

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']
    
    if not file_key.startswith("dados-posicionais/"):
        return
    
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    data = response['Body'].read().decode('utf-8').splitlines()
    
    records = []
    for line in data:
        record = {
            'numero_contrato': line[:10].strip(),
            'plano': line[10:15].strip(),
            'descricao': line[15:25].strip(),
            'id_plano': line[25:29].strip()
        }
        records.append(record)
    
    table = pa.Table.from_pylist(records)
    output_buffer = io.BytesIO()
    pq.write_table(table, output_buffer)
    output_buffer.seek(0)
    
    parquet_key = file_key.replace("dados-posicionais/", "dados-parquet/") + ".parquet"
    s3.put_object(Bucket=bucket_name, Key=parquet_key, Body=output_buffer.getvalue())
    
    return {
        'statusCode': 200,
        'body': f'Arquivo {file_key} convertido para Parquet com sucesso!'
    }
```

### 4. Criando o AWS Glue Job para Processar os Arquivos Parquet
Após a conversão, o Glue Job será responsável por:
- **Ler os arquivos Parquet**.
- **Aplicar filtros e junções**.
- **Gerar um novo Parquet com os resultados**.
- **Salvar os dados processados no OpenSearch**.

#### Código do Glue Job (PySpark)
```python
import sys
import boto3
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# Inicializando Spark e Glue
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
spark = SparkSession.builder.appName(args['JOB_NAME']).getOrCreate()
glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configurações
BUCKET_NAME = "meu-bucket-dados"
PASTA_PARQUET = "dados-parquet/"
PASTA_RESULTADOS = "dados-processados/"
OPENSEARCH_HOST = "https://meu-opensearch-endpoint"
INDEX_NAME = "meu-indice"

df = spark.read.parquet(f"s3://{BUCKET_NAME}/{PASTA_PARQUET}/")

# Aplicar filtros e transformações
df_filtrado = df.filter("plano != 'BASICO'")

# Salvar o resultado em Parquet
df_filtrado.write.parquet(f"s3://{BUCKET_NAME}/{PASTA_RESULTADOS}/", mode="overwrite")

# Enviar os dados para o OpenSearch
import requests
import json

for row in df_filtrado.collect():
    document = row.asDict()
    requests.post(f"{OPENSEARCH_HOST}/{INDEX_NAME}/_doc", headers={"Content-Type": "application/json"}, data=json.dumps(document))

job.commit()
```

### 5. Criando uma Regra no EventBridge para Verificar o Glue Job às 12h
Criamos uma regra para rodar uma verificação diária às 12h:
- **Cron Expression**: `cron(0 12 * * ? *)`.
- A Lambda verifica se o Glue Job foi concluído e, se necessário, gera um alerta.

## Conclusão
Esta solução garante um processamento eficiente dos arquivos posicionais recebidos no S3, convertendo-os para Parquet, acionando o Glue Job no momento correto e armazenando os resultados tanto em um novo Parquet quanto no OpenSearch. Além disso, o CloudWatch monitora possíveis falhas e garante que o processamento seja concluído dentro do prazo esperado.

