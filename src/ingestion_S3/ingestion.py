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
        .config("spark.executor.memory", "12g") \
        .config("spark.driver.memory", "12g") \
        .getOrCreate()


def extract(suffix):
    try:    
        path = g.glob(f"data/*.{suffix}")
        df = spark.read.csv(path, sep = ';', inferSchema = True, encoding = "latin1")
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
    suffixes = {
        "CNAECSV": "cnaes",
        "EMPRECSV": "empresas",
        "ESTABELE": "estabelecimentos",
        "MOTCSV": "motivos",
        "MUNCSV": "municipios",
        "NATJUCSV": "natureza_juridica",
        "PAISCSV": "paises",
        "QUALSCSV": "qualificacoes",
        "SOCIOCSV": "socios"
    }
    
    for suffix, filename in suffixes.items():
        df = extract(suffix)
        load(df, filename)
    
if __name__ == "__main__":
    ingestion()