from datetime import datetime
import time 

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType, IntegerType

from airflow.models.variable import Variable



postgresql_settings = {
    'user': Variable.get("P_USER"),
    'password': Variable.get("P_PASS")
}


def spark_init(test_name) -> SparkSession:
    spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )

    spark = (
        SparkSession.builder.appName(test_name)
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.jars.packages", spark_jars_packages)
            .getOrCreate()
    )
    return spark


def read_subscribers_restaurants(spark: SparkSession) -> DataFrame:
    res_df = (spark.read
                    .format("jdbc")
                    .option("url", "jdbc:postgresql://localhost:5432/de")
                    .option("dbtable", "subscribers_restaurants")
                    .option("driver", "org.postgresql.Driver")
                    .options(**postgresql_settings)
                    .load())
    return res_df


def read_adv_stream(spark: SparkSession) -> DataFrame:
    schema = StructType([
        StructField("restaurant_id", StringType()),
        StructField("adv_campaign_id", StringType()),
        StructField("adv_campaign_content", StringType()),
        StructField("adv_campaign_owner", StringType()),
        StructField("adv_campaign_owner_contact", StringType()),
        StructField("adv_campaign_datetime_start", DoubleType()),
        StructField("adv_campaign_datetime_end", DoubleType())
    ])
    df = (spark.readStream.format('kafka')
        .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091')
        .option('kafka.security.protocol', 'SASL_SSL')
        .option('kafka.sasl.mechanism', 'SCRAM-SHA-512')
        .option('kafka.sasl.jaas.config',
            f'org.apache.kafka.common.security.scram.ScramLoginModule required \username=\"{Variable.get("K_USER")}\" password=\"{Variable.get("K_PASS")}\";') \
        .option("subscribe", "zaurkokoev_in")
        .load()
        .withColumn('value', f.col('value').cast(StringType()))
        .withColumn('event', f.from_json(f.col('value'), schema))
        .selectExpr('event.*')
        .withColumn('adv_campaign_datetime_start',
            f.from_unixtime(f.col('adv_campaign_datetime_start'), "yyyy-MM-dd' 'HH:mm:ss.SSS").cast(TimestampType()))
        .withColumn('adv_campaign_datetime_end',
            f.from_unixtime(f.col('adv_campaign_datetime_end'), "yyyy-MM-dd' 'HH:mm:ss.SSS").cast(TimestampType()))
        .dropDuplicates(['restaurant_id', 'adv_campaign_id', 'adv_campaign_datetime_start'])
        .withWatermark('adv_campaign_datetime_start', '10 minutes')
        )
    return df



def foreach_batch_function(df, epoch_id):
    df = df.withColumn('trigger_datetime_created', round(time.time()))
    df.persist()
    df.write \
        .mode("append") \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/de") \
        .option('driver', 'org.postgresql.Driver') \
        .option("dbtable", "subscribers_feedback") \
        .options(**postgresql_settings)\
        .save()
    kafka_df = df.select(to_json( \
            struct("restaraunt_id", \
                   "adv_campaign_id", \
                   "adv_campaign_content", \
                   "adv_campaign_owner", \
                   "adv_campaign_owner_contact", \
                   "adv_campaign_datetime_start", \
                   "adv_campaign_datetime_end", \
                   "client_id", \
                   "datetime_created", \
                   "trigger_datetime_created")) \
            .alias("value"))
    kafka_df.write \
        .format("kafka") \
        .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
        .option('kafka.security.protocol', 'SASL_SSL') \
       .option('kafka.sasl.jaas.config',
                f'org.apache.kafka.common.security.scram.ScramLoginModule required \username=\"{Variable.get("K_USER")}\" password=\"{Variable.get("K_PASS")}\";') \
        .option('kafka.sasl.mechanism', 'SCRAM-SHA-512') \
        .option('kafka.ssl.truststore.location', '/usr/lib/jvm/java-1.17.0-openjdk-amd64/lib/security/cacerts') \
        .option('kafka.ssl.truststore.password', 'changeit') \
        .option('topic', 'zaurkokoev_out') \
        .save()
    df.unpersist()
    pass 



if __name__ == "__main__":
    spark = spark_init('subs rests zk app')
    read_stream_df = read_adv_stream(spark)
    subs_df = read_subscribers_restaurants(spark)
    res_df = read_stream_df.filter(
        read_stream_df.adv_campaign_datetime_start <= round(time.time()),
        read_stream_df.adv_campaign_datetime_end <= round(time.time())
    )\
        .join(subs_df, ['client_id', 'restaurant_id'], how = 'inner')\
        .withColumn('datetime_created', round(time.time()))\
        .select(
            'restaurant_id', 'adv_campaign_id', 'adv_campaign_content', 
            'adv_campaign_owner', 'adv_campaign_owner_contact',
            'adv_campaign_datetime_start', 'adv_campaign_datetime_end',
            'client_id', 'datetime_created'
        )
    res_df.foreachBatch(foreach_batch_function).start()
        