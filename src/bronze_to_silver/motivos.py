from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


spark = SparkSession \
        .builder \
        .appName("tabela_motivos") \
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
        columns = ["cod_motivo", "descricao"]

        df = df.toDF(*columns) \
            .dropDuplicates(subset = ['cod_motivo']) 
        
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
            
        df = extract("s3://empresas-brasil/bronze/motivos")
        df = transform(df)
        load(df, "motivos")

        logger.info("Pipeline de ETL concluído com sucesso.")
    except Exception as e:
        logger.error(f"Erro no pipeline de ETL: {e}")
        raise e

if __name__ == "__main__":
    main()
