SELECT
  scenario_id,
  scenario.name AS scenario_name,
  COALESCE(from_label.label, to_label.label) AS label,
  category.category,
  category.carrier,
  timestamp,
  value
FROM sequence
JOIN timeseries ON timeseries.sequence_id = sequence.id
JOIN scenario ON sequence.scenario_id = scenario.id
LEFT JOIN category USING(from_node, to_node)
LEFT JOIN label AS from_label ON sequence.from_node = from_label.component AND NOT from_label.is_bus
LEFT JOIN label AS to_label ON sequence.to_node = to_label.component AND NOT to_label.is_bus
ORDER BY scenario_id, label, timestamp
