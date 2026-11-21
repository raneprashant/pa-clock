from pyspark import pipelines as dp
from pyspark.sql import functions as F

EVENT_SCHEMA = ("event_id STRING, case_id STRING, event_type STRING, event_ts STRING, "
                "member_id STRING, provider_npi STRING, service_code STRING, urgency STRING, "
                "reviewer_id STRING, decision STRING")

EVENT_TYPES = "'SUBMITTED','INFO_REQUESTED','INFO_RECEIVED','DECIDED','WITHDRAWN'"
RULES = {
    "event_id_present": "event_id IS NOT NULL",
    "case_id_present":  "case_id IS NOT NULL",
    "event_ts_valid":   "event_ts IS NOT NULL",
    "event_type_known": f"event_type IN ({EVENT_TYPES})",
    "urgency_known":    "urgency IN ('STANDARD','EXPEDITED')",
}

def parsed_events():
    """Bronze JSON -> typed columns (stream)."""
    return (
        spark.readStream.table("bronze.bronze_pa_events")
        .withColumn("j", F.from_json("raw_value", EVENT_SCHEMA))
        .select("j.*", "raw_value", "kafka_ts", "ingested_at")
        .withColumn("event_ts", F.col("event_ts").cast("timestamp"))
    )

@dp.table(
    name="silver.silver_pa_events",
    comment="Valid, de-duplicated PA events enriched with line_of_business",
    table_properties={"quality": "silver"},
)
@dp.expect_all_or_drop(RULES)
def silver_pa_events():
    members = spark.read.table("ref.dim_member").select("member_id", "line_of_business")
    return (
        parsed_events()
        .withWatermark("ingested_at", "1 day")
        .dropDuplicatesWithinWatermark(["event_id"])      # stateful dedup
        .join(F.broadcast(members), "member_id", "left")  # stream-static join
        .drop("raw_value")
    )

@dp.table(
    name="silver.silver_pa_events_quarantine",
    comment="Events that failed validation, with a reason",
)
def silver_pa_events_quarantine():
    all_ok = F.expr(" AND ".join(f"({r})" for r in RULES.values()))
    return (
        parsed_events()
        .filter(~F.coalesce(all_ok, F.lit(False)))
        .withColumn("reason", F.expr(f"""
            CASE WHEN event_id IS NULL AND case_id IS NULL THEN 'unparseable_json'
                 WHEN event_id IS NULL THEN 'missing event_id'
                 WHEN case_id  IS NULL THEN 'missing case_id'
                 WHEN event_ts IS NULL THEN 'bad event_ts'
                 WHEN event_type IS NULL OR event_type NOT IN ({EVENT_TYPES}) THEN 'unknown event_type'
                 WHEN urgency IS NULL OR urgency NOT IN ('STANDARD','EXPEDITED') THEN 'unknown urgency'
                 ELSE 'other' END"""))
    )