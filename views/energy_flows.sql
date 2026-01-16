SELECT
    scenario_id,
    from_node,
    to_node,
    total_energy
FROM sequence
WHERE sequence.attribute = 'flow' and to_node NOT LIKE '%storage%' and from_node NOT LIKE '%conn%' and to_node NOT LIKE '%conn%'
ORDER BY scenario_id
