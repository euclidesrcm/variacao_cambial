from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pathlib import Path


def create_spark_session(appname, master = "local[*]"):
    return SparkSession.builder \
        .appName(appname) \
        .master(master) \
        .getOrCreate()


def main(appname, lnd_path, raw):
    spark = create_spark_session(appname)
    lnd_files = [str(f) for f in lnd_path.iterdir() if f.is_file()]
    for file in lnd_files:
        df_cambio = spark.read.option("multiline", "true").json(file)
        df_cambio.write.format('parquet').mode('append').save(f'{str(raw)}/cambio_raw')

    spark.stop()




if __name__ == '__main__':
    appname = 'raw_awesome_api'
    lnd = Path('storage/lnd')
    raw = Path('storage/raw')
    main(appname, lnd, raw)