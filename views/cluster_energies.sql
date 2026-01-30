SELECT
    scenario_id,
    scenario.name AS scenario_name,
    cluster.name,
    COALESCE(from_label.label, to_label.label) AS label,
    total_energy
FROM sequence
JOIN cluster_component USING (from_node)
JOIN cluster ON cluster_component.cluster_id = cluster.id
JOIN scenario ON sequence.scenario_id = scenario.id
LEFT JOIN label AS from_label ON sequence.from_node = from_label.component AND NOT from_label.is_bus
LEFT JOIN label AS to_label ON sequence.to_node = to_label.component AND NOT to_label.is_bus
WHERE sequence.attribute = 'flow'
ORDER BY scenario_id, cluster.name
