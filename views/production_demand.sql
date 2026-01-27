SELECT
    scenario_id,
    scenario.name AS scenario_name,
    from_node,
    to_node,
    label.label,
    category.category,
    total_energy
FROM sequence
JOIN scenario ON sequence.scenario_id = scenario.id
LEFT JOIN label USING(from_node, to_node)
LEFT JOIN category USING(from_node, to_node)
WHERE sequence.attribute = 'flow' and to_node NOT LIKE '%storage%' and from_node NOT LIKE '%conn%' and to_node NOT LIKE '%conn%'
ORDER BY scenario_id
