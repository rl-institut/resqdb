SELECT
    scenario_id,
    scenario.name AS scenario_name,
    from_node,
    to_node,
    COALESCE(from_label.label, to_label.label) AS label,
    category.category,
    category.carrier,
    total_energy
FROM sequence
JOIN scenario ON sequence.scenario_id = scenario.id
LEFT JOIN label AS from_label ON sequence.from_node = from_label.component AND NOT from_label.is_bus
LEFT JOIN label AS to_label ON sequence.to_node = to_label.component AND NOT to_label.is_bus
LEFT JOIN category USING(from_node, to_node)
WHERE sequence.attribute = 'flow' and to_node NOT LIKE ALL(ARRAY['%storage%', '%battery%', '%conn%']) and from_node NOT LIKE ALL(ARRAY['%storage%', '%battery%', '%conn%'])
ORDER BY scenario_id
