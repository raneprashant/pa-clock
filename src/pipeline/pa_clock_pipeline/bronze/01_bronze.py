from pyspark import pipelines as dp
from pyspark.sql import functions as F

SCOPE = "pa_clock_scope"
TOPIC = "pa-events"

def kafka_options():
    key = dbutils.secrets.get(SCOPE, "kafka-api-key")
    secret = dbutils.secrets.get(SCOPE, "kafka-api-secret")
    jaas = ('kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
            f'username="{key}" password="{secret}";')
    return {
        "kafka.bootstrap.servers": dbutils.secrets.get(SCOPE, "kafka-bootstrap"),
        "subscribe": TOPIC,
        "startingOffsets": "earliest",
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": jaas,
    }

@dp.table(
    name="pa_clock_dev.bronze.bronze_pa_events",
    comment="Raw PA events from Kafka topic pa-events (no cleaning: duplicates and bad records included)",
    table_properties={"quality": "bronze"},
)
def bronze_pa_events():
    return (
        spark.readStream.format("kafka").options(**kafka_options()).load()
        .select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("raw_value"),
            F.col("topic"), F.col("partition"), F.col("offset"),
            F.col("timestamp").alias("kafka_ts"),
            F.current_timestamp().alias("ingested_at"),
        )
    )