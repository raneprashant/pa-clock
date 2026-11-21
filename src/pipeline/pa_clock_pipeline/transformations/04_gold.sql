CREATE OR REFRESH MATERIALIZED VIEW pa_clock_dev.gold.gold_pa_open_cases_at_risk
COMMENT 'Open PA cases with hours remaining and risk band (recomputed on every refresh)'
AS
WITH t AS (
  SELECT
    case_id, member_id, provider_npi, service_code, urgency, line_of_business,
    submitted_ts, due_ts, hours_allowed, warn_pct, critical_pct,
    current_timestamp() AS as_of_ts,
    (unix_timestamp(current_timestamp()) - unix_timestamp(submitted_ts)) / 3600.0 AS hours_elapsed
  FROM pa_clock_dev.silver.silver_pa_cases
  WHERE status = 'OPEN' AND due_ts IS NOT NULL
)
SELECT
  case_id, member_id, provider_npi, service_code, urgency, line_of_business,
  submitted_ts, due_ts, hours_allowed,
  round(hours_elapsed, 1)                  AS hours_elapsed,
  round(hours_allowed - hours_elapsed, 1)  AS hours_remaining,
  round(hours_elapsed / hours_allowed, 3)  AS pct_elapsed,
  CASE WHEN hours_elapsed >= hours_allowed                THEN 'BREACHED'
       WHEN hours_elapsed >= critical_pct * hours_allowed THEN 'CRITICAL'
       WHEN hours_elapsed >= warn_pct * hours_allowed     THEN 'WARNING'
       ELSE 'ON_TRACK' END                 AS risk_band,
  as_of_ts
FROM t;

CREATE OR REFRESH MATERIALIZED VIEW pa_clock_dev.gold.gold_pa_compliance_daily
COMMENT 'Daily decided-case metrics by line of business and urgency'
AS
WITH d AS (
  SELECT
    to_date(decided_ts) AS decision_date, line_of_business, urgency, decision,
    (unix_timestamp(decided_ts) - unix_timestamp(submitted_ts)) / 3600.0 AS turnaround_hours,
    decided_ts <= due_ts AS is_on_time
  FROM pa_clock_dev.silver.silver_pa_cases
  WHERE status = 'DECIDED' AND submitted_ts IS NOT NULL AND due_ts IS NOT NULL
)
SELECT
  decision_date, line_of_business, urgency,
  count(*)                                             AS decided_cases,
  count_if(decision = 'APPROVED')                      AS approved,
  count_if(decision = 'DENIED')                        AS denied,
  count_if(decision = 'PARTIAL')                       AS partially_approved,
  round(avg(turnaround_hours), 1)                      AS avg_turnaround_hours,
  round(percentile_approx(turnaround_hours, 0.5), 1)   AS median_turnaround_hours,
  count_if(is_on_time)                                 AS decided_on_time,
  round(100.0 * count_if(is_on_time) / count(*), 1)    AS on_time_pct
FROM d
GROUP BY decision_date, line_of_business, urgency;