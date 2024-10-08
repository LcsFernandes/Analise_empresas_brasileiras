﻿from pyspark.sql import SparkSession 
from pyspark.sql.functions import *
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


spark = SparkSession \
        .builder \
        .appName("tabela_estabelecimentos") \
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
        
        columns = ["cnpj", "cnpj_ordem", "cnpj_verificador", "identificador_mtz_fil", "nome", "st_cadastral", "dt_st_cadastral", "cod_motivo", "nm_cid_exterior", "cod_pais", "dt_inicio", "cod_cnaes", "cod_cnaes2", "tp_logradouro", "logradouro", "numero", "complemento", "bairro", "cep", "uf", "cod_municipio", "ddd1", "telefone1", "ddd2", "telefone2", "ddd_fax", "fax", "email", "st_especial", "dt_st_especial"]
        ufs_brasil = ["AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"]


        df = df.toDF(*columns) \
            .filter(regexp_extract(col("st_cadastral"), r'^\d{2}$', 0) != "") \
            .filter(col("dt_inicio").isNotNull()) \
            .filter(col("cod_cnaes").isNotNull()) \
            .filter(col("cod_municipio").isNotNull()) \
            .filter(upper(col("uf")).isin(ufs_brasil)) \
            .withColumns({
                "dt_st_cadastral": to_date(col("dt_st_cadastral"), 'yyyyMMdd'),
                "dt_inicio": to_date(col("dt_inicio"), 'yyyyMMdd'),
                "dt_st_especial": to_date(col("dt_st_especial"), 'yyyyMMdd'),
                "st_cadastral": col("st_cadastral").cast("integer")
            }) \
            .filter(
                (year(col("dt_st_cadastral")) >= 1900) & (year(col("dt_inicio")) >= 1900)
            )
        
        df = df.repartition("uf")
        
        
        logger.info("Transformação concluída.")
        return df
    except Exception as e:
        logger.error(f"Erro ao transformar os dados: {e}")
        raise e

def load(df, filename):
    try:
        path = f"s3://empresas-brasil/silver/{filename}"
        logger.info(f"Carregando dados para o caminho: {path}")

        df.write.parquet(path, mode = 'overwrite')

        logger.info(f"Dados carregados com sucesso em: {path}")
    except Exception as e:
        logger.error(f"Erro ao carregar os dados para {filename}: {e}")
        raise e
        
def main():
    try:
        logger.info("Iniciando pipeline ETL")
        
        df = extract("s3://empresas-brasil/bronze/estabelecimentos")
        df = transform(df)
        load(df, "estabelecimentos")
        
        logger.info("Pipeline de ETL concluído com sucesso.")
    except Exception as e:
        logger.error(f"Erro no pipeline de ETL: {e}")
        raise e

if __name__ == "__main__":
    main()
