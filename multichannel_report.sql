WITH
  sourse_table AS (
  SELECT
    event_timestamp,
    event_name,
    user_pseudo_id,
    traffic_source.source,
    traffic_source.medium,
    params.value.int_value AS session_number
  FROM
    `sound-vault-327810.analytics_288444991.events_*`,
    UNNEST(event_params) AS params
  WHERE
    event_name = "session_start"
    AND params.key = "ga_session_number"),
  lead_table AS (
  SELECT
    sourse_table.event_timestamp,
    sourse_table.event_name,
    sourse_table.user_pseudo_id,
    CONCAT(sourse_table.source, ' / ',sourse_table.medium) as source_medium,
    sourse_table.session_number,
    GA.leads AS lead
  FROM
    sourse_table
  LEFT JOIN (
    SELECT
      user_pseudo_id,
      params.value.int_value AS session_number,
      COUNT(event_name) AS leads
    FROM
      `sound-vault-327810.analytics_288444991.events_*`,
      UNNEST(event_params) AS params
    WHERE
      params.key = 'ga_session_number'
      AND event_name ='Lead'
    GROUP BY
      user_pseudo_id,
      session_number) AS GA
  ON
    sourse_table.user_pseudo_id = GA.user_pseudo_id
    AND sourse_table.session_number = GA.session_number ),
multichannel as(
    SELECT STRING_AGG(source_medium, ' > ') OVER (PARTITION BY user_pseudo_id ORDER BY session_number ROWS UNBOUNDED PRECEDING) AS multichannel,
    SUM(lead) as sum_lead
    FROM lead_table GROUP by user_pseudo_id, source_medium, session_number)
SELECT
  *
FROM
  multichannel order by sum_lead desc