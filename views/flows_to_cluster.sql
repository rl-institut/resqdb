SELECT
    scenario_id,
    cluster.name,
    from_node AS technology,
    (SELECT SUM(t) FROM unnest(timeseries) AS t) AS energy
FROM cluster
JOIN flow ON cluster.id = flow.cluster_id
WHERE flow.attribute = 'flow'
ORDER BY scenario_id, cluster.name;
