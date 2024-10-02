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
        .appName("tabela_socios") \
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
        columns = ["cnpj", "identificador_socio", "nome", "cpf_cnpj_socio", "cod_qualificacao", "dt_entrada", "cod_pais", "representante_legal", "nm_representante", "cod_qualificacao_rp", "faixa_etaria"]

        df = df.toDF(*columns) \
            .withColumns({
                "dt_entrada": to_date(col("dt_entrada"), "yyyyMMdd"),
                "idade": when(col("faixa_etaria") == 1, "0-12") \
                        .when(col("faixa_etaria") == 2, "13-20") \
                        .when(col("faixa_etaria") == 3, "21-30") \
                        .when(col("faixa_etaria") == 4, "31-40") \
                        .when(col("faixa_etaria") == 5, "41-50") \
                        .when(col("faixa_etaria") == 6, "51-60") \
                        .when(col("faixa_etaria") == 7, "61-70") \
                        .when(col("faixa_etaria") == 8, "71-80") \
                        .when(col("faixa_etaria") == 9, "80+") \
                        .otherwise("Não Se Aplica"),
                "classificacao_socio": when(col("identificador_socio") == 1, "PESSOA JURIDICA") \
                                       .when(col("identificador_socio") == 2, "PESSOA FISICA") \
                                       .otherwise("ESTRANGEIRO")
                }) \
                .filter((year(col("dt_entrada")) <= 2024) & (year(col("dt_entrada")) >= 1900)) 
        
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

        df = extract("s3a://empresas-brasil/bronze/socios.parquet")
        df = transform(df)
        load(df, "socios.parquet")

        logger.info("Pipeline de ETL concluído com sucesso.")
    except Exception as e:
        logger.error(f"Erro no pipeline de ETL: {e}")
        raise e
        
if __name__ == "__main__":
    main()

