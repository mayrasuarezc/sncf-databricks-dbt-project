SELECT
  stop_id,
  stop_name,
  COUNT(DISTINCT trip_id) AS number_of_trips,
  COUNT(DISTINCT route_id) AS number_of_routes
FROM {{ source('transport', 'silver_trip_stops') }}
GROUP BY stop_id, stop_name
ORDER BY number_of_trips DESC