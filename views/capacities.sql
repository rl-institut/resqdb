SELECT
    scenario_id,
    scenario.name AS scenario_name,
    COALESCE(from_label.label, to_label.label) AS label,
    COALESCE(MAX(CASE WHEN attribute = 'total' THEN value END), MAX(CASE WHEN attribute = 'capacity' THEN value END)) AS capacity
FROM
  scalar
  JOIN scenario ON scalar.scenario_id = scenario.id
  LEFT JOIN label AS from_label ON scalar.from_node = from_label.component AND NOT from_label.is_bus AND from_label.label NOT LIKE 'Abregelung%'
  LEFT JOIN label AS to_label ON scalar.to_node = to_label.component AND NOT to_label.is_bus
WHERE (attribute = 'capacity' OR attribute = 'total') AND COALESCE(from_label.label, to_label.label) IS NOT NULL
GROUP BY scenario_id, scenario.name, COALESCE(from_label.label, to_label.label)
