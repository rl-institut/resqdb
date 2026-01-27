SELECT
    scenario_id,
    scenario.name AS scenario_name,
    COALESCE(from_label.label, to_label.label) AS label,
    COALESCE(MAX(CASE WHEN attribute = 'total' THEN value END), MAX(CASE WHEN attribute = 'capacity' THEN value END)) AS capacity
FROM
  scalar
  JOIN scenario ON scalar.scenario_id = scenario.id
  LEFT JOIN label AS from_label ON scalar.from_node = from_label.component
  LEFT JOIN label AS to_label ON scalar.to_node = to_label.component
WHERE attribute = 'capacity' OR attribute = 'total' AND NOT from_label.is_bus AND NOT to_label.is_bus
GROUP BY scenario_id, scenario.name, COALESCE(from_label.label, to_label.label)
