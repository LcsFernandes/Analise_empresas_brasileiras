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

def socios_por_classificacao(df_socios):
    try:
        logger.info("Iniciando socios_por_classificacao")
        
        qtd_socios_por_classificacao = df_socios.select("classificacao_socio") \
                                .groupBy("classificacao_socio") \
                                .count() \
                                .withColumnRenamed("count", "Total_de_socios") \
                                .orderBy(desc("Total_de_socios"))
        
        logger.info("Transformação concluída.")
        return qtd_socios_por_classificacao
    
    except Exception as e:
        logger.error(f"Erro ao transformar os dados: {e}")
        raise e
    
def paises_estrangeiros(df_socios, df_paises):
    try:
        logger.info("Iniciando paises_estrangeiros")

        paises_estrangeiros = df_socios.join(df_paises, on = df_socios.cod_pais == df_paises.cod_pais) \
                                .select(df_socios.cod_pais, "descricao") \
                                .groupBy(df_socios.cod_pais, "descricao") \
                                .count() \
                                .withColumnsRenamed({
                                    "count": "Total_de_socios",
                                    "descricao": "Pais"
                                }) \
                                .orderBy(desc("Total_de_socios")) 
        
        logger.info("Transformação concluída.")
        return paises_estrangeiros
    except Exception as e:
        logger.error(f"Erro ao transformar os dados: {e}")
        raise e    
    

def faixa_etaria(df_socios):
    try:
        logger.info("Iniciando faixa_etaria")

        faixa_etaria = df_socios.select("faixa_etaria", "idade") \
            .groupBy("faixa_etaria", "idade") \
            .count() \
            .withColumnRenamed("count", "Total_de_socios") \
            .orderBy(desc("Total_de_socios")) 
        
        logger.info("Transformação concluída.")
        return faixa_etaria
    except Exception as e:
        logger.error(f"Erro ao transformar os dados: {e}")
        raise e 
        
def socios_por_periodo(df_socios):
    try:
        logger.info("Iniciando socios_por_periodo")
        socios_por_periodo = df_socios.select("dt_entrada") \
                        .withColumn("ano_mes", date_format("dt_entrada", "yyyy-MM")) \
                        .groupBy("ano_mes") \
                        .count() \
                        .withColumnRenamed("count", "Total_de_socios") \
                        .orderBy(desc("Total_de_socios"))
        
        logger.info("Transformação concluída.")
        return socios_por_periodo
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
        
        df_socios = extract("s3a://empresas-brasil/silver/socios")
        df_paises = extract("s3a://empresas-brasil/silver/paises")

        df_socios_por_classificacao = socios_por_classificacao(df_socios)
        df_paises_estrangeiros = paises_estrangeiros(df_socios, df_paises)
        df_faixa_etaria = faixa_etaria(df_socios)
        df_socios_por_periodo = socios_por_periodo(df_socios)
   
        load(df_socios_por_classificacao, "socios_por_classificacao")
        load(df_paises_estrangeiros, "paises_estrangeiros")
        load(df_faixa_etaria, "faixa_etaria")
        load(df_socios_por_periodo, "socios_por_periodo")


        logger.info("Pipeline de ETL concluído com sucesso.")
    except Exception as e:
        logger.error(f"Erro no pipeline de ETL: {e}")
        raise e
    
if __name__ == "__main__":
    main()
    