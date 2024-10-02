﻿from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os 
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")

if not AWS_ACCESS_KEY or not AWS_SECRET_KEY:
    raise ValueError("As credenciais AWS não estão definidas corretamente no arquivo .env")


spark = SparkSession \
        .builder \
        .appName("tabela_municipios") \
        .config("fs.s3a.access.key", AWS_ACCESS_KEY) \
        .config("fs.s3a.secret.key", AWS_SECRET_KEY) \
        .getOrCreate()

def extract(path):
    try:
        logger.info(f"Extraindo dados do caminho: {path}")
        
        df = spark.read.parquet(path)
        
        logger.info(f"Extração concluída. Total de registros: {df.count()}")
        return df
    except Exception as e:
        logger.error(f"Erro ao extrair dados do caminho {path}: {e}")
        raise e
    
def transform(df):
    try:
        logger.info("Iniciando transformação dos dados")
        columns = ["cod_municipio", "descricao"]

        df = df.toDF(*columns) \
            .dropDuplicates(subset = ['cod_municipio']) 
        
        logger.info("Transformação concluída.")
        return df
    except Exception as e:
        logger.error(f"Erro ao transformar os dados: {e}")
        raise e

def load(df, filename):
    try:
        path = f"s3a://empresas-brasil/silver/{filename}"
        logger.info(f"Carregando dados para o caminho: {path}")
        df.write.parquet(path, mode = 'overwrite')
        
        logger.info(f"Dados carregados com sucesso em: {path}")
    except Exception as e:
        logger.error(f"Erro ao carregar os dados para {filename}: {e}")
        raise e
        

def main():
    try:
        logger.info("Iniciando pipeline ETL")    
        df = extract("s3a://empresas-brasil/bronze/municipios.parquet")
        df = transform(df)
        load(df, "municipios.parquet")

        logger.info("Pipeline de ETL concluído com sucesso.")
    except Exception as e:
        logger.error(f"Erro no pipeline de ETL: {e}")
        raise e

if __name__ == "__main__":
    main()