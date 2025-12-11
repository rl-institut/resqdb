SELECT
    scenario_id,
    from_node,
    to_node,
    (SELECT SUM(t) FROM unnest(timeseries) AS t) AS energy
FROM flow
WHERE flow.attribute = 'flow' and to_node NOT LIKE '%storage%' and from_node NOT LIKE '%conn%' and to_node NOT LIKE '%conn%'
ORDER BY scenario_id
