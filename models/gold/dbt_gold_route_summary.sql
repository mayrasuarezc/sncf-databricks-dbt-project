SELECT
  route_id,
  route_short_name,
  route_long_name,
  COUNT(DISTINCT trip_id) AS number_of_trips,
  COUNT(DISTINCT stop_id) AS number_of_stops,
  COUNT(*) AS number_of_stop_records
FROM {{ source('transport', 'silver_trip_stops') }}
GROUP BY route_id, route_short_name, route_long_name
ORDER BY number_of_trips DESC