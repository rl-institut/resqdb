SELECT
    scenario_id,
    cluster.name,
    label.label,
    total_energy
FROM cluster
JOIN sequence ON cluster.id = sequence.cluster_id
LEFT JOIN label USING (from_node, to_node)
WHERE sequence.attribute = 'flow'
ORDER BY scenario_id, cluster.name;
