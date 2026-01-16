SELECT
    scenario_id,
    cluster.name,
    from_node AS technology,
    total_energy AS energy
FROM cluster
JOIN sequence ON cluster.id = sequence.cluster_id
WHERE sequence.attribute = 'flow'
ORDER BY scenario_id, cluster.name;
