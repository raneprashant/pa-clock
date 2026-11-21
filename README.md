# PA Clock
Prior-authorization turnaround monitor: Kafka → Lakeflow Spark Declarative Pipelines → Delta (bronze/silver/gold).
Layers: ref, bronze, silver, gold. Decision windows per CMS-0057-F (72h expedited / 7 days standard).


## Runtime setup (Databricks UI, not in Git)
- Pipeline `pa_clock_pipeline`: root `src/pipeline`, source `src/pipeline/transformations`, default schema `bronze`, serverless.
- Job `pa_clock_alerts_job`: task 1 `refresh_pipeline` (pipeline), task 2 `send_alerts` (notebook `src/jobs/01_send_alerts`, params: catalog, send_email).
- Secret scope `pa_clock_scope`: kafka-bootstrap, kafka-api-key, kafka-api-secret, smtp-user, smtp-app-password, alert-to.