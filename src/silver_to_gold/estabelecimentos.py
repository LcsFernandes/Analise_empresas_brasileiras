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

def total_estabelecimentos_por_st_cadastral(df_estabelecimentos):
    try:
        logger.info("Iniciando total_estabelecimentos_por_st_cadastral")
        
        df_estabelecimentos = df_estabelecimentos.withColumn("descricao_st_cadastral", 
                                when(col("st_cadastral") == 1, "Nula") \
                                .when(col("st_cadastral") == 2, "Ativa") \
                                .when(col("st_cadastral") == 3, "Suspensa") \
                                .when(col("st_cadastral") == 4, "Inapta") \
                                .otherwise("Baixada")
                                )
        df_total_estabelecimentos_por_st_cadastral = df_estabelecimentos.groupBy("st_cadastral", "descricao_st_cadastral") \
                                                                        .count() \
                                                                        .withColumnRenamed("count", "Total") \
                                                                        .orderBy(desc("Total"))
        
        logger.info("Transformação concluída.")
        return df_total_estabelecimentos_por_st_cadastral
    
    except Exception as e:
        logger.error(f"Erro ao transformar os dados: {e}")
        raise e
    
def novos_estabelecimentos_por_periodo(df_estabelecimentos):
    try:
        logger.info("Iniciando novos_estabelecimentos_por_periodo")

        df_novos_estabelecimentos_por_periodo = df_estabelecimentos.select("dt_inicio") \
                        .withColumn("ano_mes", date_format("dt_inicio", "yyyy-MM")) \
                        .groupBy("ano_mes") \
                        .count() \
                        .withColumnRenamed("count", "Total") \
                        .orderBy(desc("ano_mes"))
                            
        
        logger.info("Transformação concluída.")
        return df_novos_estabelecimentos_por_periodo
    except Exception as e:
        logger.error(f"Erro ao transformar os dados: {e}")
        raise e    
    
def principais_atividades_economicas(df_estabelecimentos, df_cnaes):
    try:
        logger.info("Iniciando principais_atividades_economicas")

        df_principais_atividades_economicas = df_estabelecimentos.join(df_cnaes, on = df_estabelecimentos.cod_cnaes == df_cnaes.cod_cnaes) \
                    .groupBy(df_estabelecimentos.cod_cnaes, df_cnaes.descricao) \
                    .count() \
                    .withColumnRenamed("count", "Total") \
                    .orderBy(desc("Total"))
                            
        
        logger.info("Transformação concluída.")
        return df_principais_atividades_economicas
    except Exception as e:
        logger.error(f"Erro ao transformar os dados: {e}")
        raise e  
    
def estabelecimentos_por_estado(df_estabelecimentos):
    try:
        logger.info("Iniciando estabelecimentos_por_estado")

        df_estabelecimentos_por_estado = df_estabelecimentos.groupBy("uf") \
                                                            .count() \
                                                            .withColumnRenamed("count", "Total") \
                                                            .orderBy(desc("Total"))
        
        logger.info("Transformação concluída.")
        return df_estabelecimentos_por_estado
    except Exception as e:
        logger.error(f"Erro ao transformar os dados: {e}")
        raise e 

def estabelecimentos_por_municipio(df_estabelecimentos, df_municipios):
    try:
        logger.info("Iniciando estabelecimentos_por_municipio")

        df_estabelecimentos_por_municipio = df_estabelecimentos.join(df_municipios, on = df_estabelecimentos.cod_municipio == df_municipios.cod_municipio) \
                                                            .groupBy("uf", df_estabelecimentos.cod_municipio, df_municipios.descricao) \
                                                            .count() \
                                                            .withColumnsRenamed({
                                                                "count": "Total_estabelecimentos",
                                                                "descricao": "municipio"
                                                                }) \
                                                            .orderBy(desc("Total_estabelecimentos"))
                    
        
        logger.info("Transformação concluída.")
        return df_estabelecimentos_por_municipio
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
        
        df_estabelecimentos = extract("s3a://empresas-brasil/silver/estabelecimentos")
        df_municipios = extract("s3a://empresas-brasil/silver/municipios")
        df_cnaes = extract("s3a://empresas-brasil/silver/cnaes")

        df_total_estabelecimentos_por_st_cadastral = total_estabelecimentos_por_st_cadastral(df_estabelecimentos)
        df_novos_estabelecimentos_por_periodo = novos_estabelecimentos_por_periodo(df_estabelecimentos)
        df_principais_atividades_economicas = principais_atividades_economicas(df_estabelecimentos, df_cnaes)
        df_estabelecimentos_por_estado = estabelecimentos_por_estado(df_estabelecimentos)
        df_estabelecimentos_por_municipio = estabelecimentos_por_municipio(df_estabelecimentos, df_municipios)

   
        load(df_total_estabelecimentos_por_st_cadastral, "estabelecimentos_por_st_cadastral")
        load(df_novos_estabelecimentos_por_periodo, "novos_estabelecimentos_por_periodo")
        load(df_principais_atividades_economicas, "principais_atividades_economicas")
        load(df_estabelecimentos_por_estado, "estabelecimentos_por_estado")
        load(df_estabelecimentos_por_municipio, "estabelecimentos_por_municipio")

        logger.info("Pipeline de ETL concluído com sucesso.")
    except Exception as e:
        logger.error(f"Erro no pipeline de ETL: {e}")
        raise e
    
if __name__ == "__main__":
    main()
    