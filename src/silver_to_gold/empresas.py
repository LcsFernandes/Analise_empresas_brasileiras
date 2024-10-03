from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os 
from pyspark.sql.functions import *
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
        .appName("fat_socios") \
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

def total_por_porte(df_empresas):
    try:
        logger.info("Iniciando socios_por_classificacao")
        
        df_total_por_porte = df_empresas.groupBy("porte_empresa") \
                            .count() \
                            .withColumnRenamed("count", "Total_de_empresas") \
                            .orderBy(desc("Total_de_empresas"))
        
        
        logger.info("Transformação concluída.")
        return df_total_por_porte
    
    except Exception as e:
        logger.error(f"Erro ao transformar os dados: {e}")
        raise e
    
def total_por_natureza_juridica(df_empresas, df_natureza_juridica):
    try:
        logger.info("Iniciando paises_estrangeiros")
        df_total_por_natureza_juridica = df_empresas.join(df_natureza_juridica, on = df_empresas.cod_natureza_juridica == df_natureza_juridica.cod_natureza_juridica) \
                                                    .select(df_empresas.cod_natureza_juridica, df_natureza_juridica.descricao) \
                                                    .groupBy(df_empresas.cod_natureza_juridica, df_natureza_juridica.descricao) \
                                                    .count() \
                                                    .withColumnsRenamed({
                                                        "count": "Total"
                                                        }) \
                                                    .orderBy(desc("Total"))
        
        
        logger.info("Transformação concluída.")
        return df_total_por_natureza_juridica
    except Exception as e:
        logger.error(f"Erro ao transformar os dados: {e}")
        raise e    
    
                        
def load(df, filename):
    try:
        path = f"s3a://empresas-brasil/gold/{filename}"
        logger.info(f"Carregando dados para o caminho: {path}")
        df.write.parquet(path, mode = 'overwrite')
        
        logger.info(f"Dados carregados com sucesso em: {path}")
    except Exception as e:
        logger.error(f"Erro ao carregar os dados para {filename}: {e}")
        raise e

def main():
    try:
        logger.info("Iniciando pipeline ETL")
        
        df_empresas = extract("s3a://empresas-brasil/silver/empresas")
        df_natureza_juridica = extract("s3a://empresas-brasil/silver/natureza_juridica")

        df_total_por_porte = total_por_porte(df_empresas)
        df_total_por_natureza_juridica = total_por_natureza_juridica(df_empresas, df_natureza_juridica)
       
   
        load(df_total_por_porte, "total_por_porte")
        load(df_total_por_natureza_juridica, "total_por_natureza_juridica")


        logger.info("Pipeline de ETL concluído com sucesso.")
    except Exception as e:
        logger.error(f"Erro no pipeline de ETL: {e}")
        raise e
    
if __name__ == "__main__":
    main()
    