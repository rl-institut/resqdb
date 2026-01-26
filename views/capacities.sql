SELECT
    scenario_id,
    label,
    COALESCE(MAX(CASE WHEN attribute = 'total' THEN value END), MAX(CASE WHEN attribute = 'capacity' THEN value END)) AS capacity
FROM
  scalar
  JOIN label ON scalar.from_node = label.from_node
  AND (
    scalar.to_node = label.to_node
    OR (
      scalar.to_node IS NULL
      AND label.to_node IS NULL
    )
  )
WHERE attribute = 'capacity' OR attribute = 'total'
GROUP BY scenario_id, label
