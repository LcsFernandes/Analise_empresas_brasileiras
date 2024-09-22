from pyspark.sql import SparkSession
from dotenv import load_dotenv
import glob as g
import os
import logging


load_dotenv()

logging.basicConfig(level=logging.INFO)

spark = SparkSession \
        .builder \
        .appName("Ingestion_S3") \
        .config("fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY")) \
        .config("fs.s3a.secret.key", os.getenv("AWS_SECRET_KEY")) \
        .getOrCreate()


def extract(suffix):
    try:    
        path = g.glob(f"data/*.{suffix}")
        df = spark.read.csv(path, sep = ';')
        logging.info(f"Extracted data from {path}")
        return df
    except Exception as e:
        logging.error(f"Error during extraction: {e}")
        raise e
    
def load(df, filename):
    try:
        df.write.parquet(f"s3a://empresas-brasil/bronze/{filename}", mode = 'overwrite')
        logging.info(f"Loaded data to s3a://empresas-brasil/bronze/{filename}")
    except Exception as e:
        logging.error(f"Error during loading: {e}")
        raise e


def ingestion():
    df_cnaes = extract("CNAECSV")
    df_empresas = extract("EMPRECSV")
    df_estabelecimentos = extract("ESTABELE")
    df_motivos = extract("MOTCSV")
    df_municipios = extract("MUNCSV")
    df_natureza_juridica = extract("NATJUCSV")
    df_paises = extract("PAISCSV")
    df_qualificacoes = extract("QUALSCSV")
    df_simples_nacional = extract("D40608")
    df_socios = extract("SOCIOCSV")

    load(df_cnaes, "cnaes.parquet")
    load(df_empresas, "empresas.parquet")
    load(df_estabelecimentos, "estabelecimentos.parquet")
    load(df_motivos, "motivos.parquet")
    load(df_municipios, "municipios.parquet")
    load(df_natureza_juridica, "natureza_juridica.parquet")
    load(df_paises, "paises.parquet")
    load(df_qualificacoes, "qualificacoes.parquet")
    load(df_simples_nacional, "simples_nacional.parquet")
    load(df_socios, "socios.parquet")
    
if __name__ == "__main__":
    ingestion()