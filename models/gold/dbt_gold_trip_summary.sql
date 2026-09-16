WITH ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY trip_id ORDER BY stop_sequence ASC) AS rn_asc,
    ROW_NUMBER() OVER (PARTITION BY trip_id ORDER BY stop_sequence DESC) AS rn_desc
  FROM {{ source('transport', 'silver_trip_stops') }}
)

SELECT
  trip_id,
  route_id,
  route_short_name,
  MAX(CASE WHEN rn_asc = 1 THEN stop_name END) AS first_stop,
  MAX(CASE WHEN rn_desc = 1 THEN stop_name END) AS last_stop,
  MAX(stop_sequence) AS number_of_stops
FROM ranked
GROUP BY trip_id, route_id, route_short_name