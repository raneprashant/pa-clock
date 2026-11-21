CREATE OR REFRESH MATERIALIZED VIEW silver.silver_pa_cases
COMMENT 'One row per PA case, rebuilt from all events (robust to late and out-of-order events)'
AS
WITH c AS (
  SELECT
    case_id,
    min(member_id)        AS member_id,
    min(provider_npi)     AS provider_npi,
    min(service_code)     AS service_code,
    min(urgency)          AS urgency,
    min(line_of_business) AS line_of_business,
    min(CASE WHEN event_type = 'SUBMITTED' THEN event_ts END) AS submitted_ts,
    count_if(event_type = 'INFO_REQUESTED')                   AS info_requests,
    max(CASE WHEN event_type = 'DECIDED' THEN event_ts END)   AS decided_ts,
    max_by(CASE WHEN event_type = 'DECIDED' THEN decision END,
           CASE WHEN event_type = 'DECIDED' THEN event_ts END) AS decision,
    max(CASE WHEN event_type = 'WITHDRAWN' THEN event_ts END) AS withdrawn_ts,
    max(event_ts)                                             AS last_event_ts,
    count(*)                                                  AS event_count
  FROM silver.silver_pa_events
  GROUP BY case_id
)
SELECT
  c.*,
  CASE WHEN c.decided_ts IS NOT NULL THEN 'DECIDED'
       WHEN c.withdrawn_ts IS NOT NULL THEN 'WITHDRAWN'
       ELSE 'OPEN' END AS status,
  r.hours_allowed, r.warn_pct, r.critical_pct,
  c.submitted_ts + make_interval(0, 0, 0, 0, r.hours_allowed, 0, 0) AS due_ts
FROM c
LEFT JOIN ref.ref_pa_rules r
  ON  c.line_of_business = r.line_of_business
  AND c.urgency = r.urgency
  AND to_date(c.submitted_ts) >= r.effective_from
  AND (r.effective_to IS NULL OR to_date(c.submitted_ts) <= r.effective_to);