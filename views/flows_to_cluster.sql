SELECT
    scenario_id,
    scenario.name AS scenario_name,
    cluster.name,
    label.label,
    total_energy
FROM cluster
JOIN sequence ON cluster.id = sequence.cluster_id
JOIN scenario ON sequence.scenario_id = scenario.id
LEFT JOIN label USING (from_node, to_node)
WHERE sequence.attribute = 'flow'
ORDER BY scenario_id, cluster.name;
