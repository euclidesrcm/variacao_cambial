from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pathlib import Path
from datetime import datetime


def create_spark_session(appname, master = "local[*]"):
    return SparkSession.builder \
        .appName(appname) \
        .master(master) \
        .getOrCreate()


def main(appname, raw, trs, data_processing_date):
    spark = create_spark_session(appname)
    df_cambio = spark.read.parquet(f'{raw}/cambio_raw')

    drop_columns = ['name', 'varBid', 'timestamp']

    df_cambio.filter(F.col('create_date').contains(data_processing_date))

    df_cambio = df_cambio.withColumnRenamed('code', 'cod_moeda_base')\
                    .withColumnRenamed('codein', 'cod_moeda_conversao')\
                    .withColumnRenamed('bid', 'vl_compra')\
                    .withColumnRenamed('ask', 'vl_venda')\
                    .withColumnRenamed('pctChange', 'pct_variacao_ultimo_fechamento')\
                    .withColumnRenamed('high', 'vl_maior_cotacao_dia')\
                    .withColumnRenamed('low', 'vl_menor_cotacao_dia')\
                    .withColumnRenamed('create_date', 'dt_fechamento')\
                    .drop(*drop_columns)
    
    df_cambio = df_cambio.withColumns(
        {
            'vl_compra': F.col('vl_compra').cast(T.DecimalType(6,6)),
            'vl_venda': F.col('vl_venda').cast(T.DecimalType(6,6)),
            'pct_variacao_ultimo_fechamento': F.col('pct_variacao_ultimo_fechamento').cast(T.DecimalType(6,6)),
            'vl_maior_cotacao_dia': F.col('vl_maior_cotacao_dia').cast(T.DecimalType(6,6)),
            'vl_menor_cotacao_dia': F.col('vl_menor_cotacao_dia').cast(T.DecimalType(6,6)),
            'dt_fechamento': F.col("dt_fechamento").cast(T.TimestampType())
        }
    )

    df_cambio.write.format('parquet').mode('append').save(f'{str(trs)}/cambio_trs')

    spark.stop()


if __name__ == '__main__':
    appname = 'trs_awesome_api'
    data_processing_date = datetime.today().date()
    raw = Path('storage/raw')
    trs = Path('storage/trs')
    main(appname, raw, trs, data_processing_date)