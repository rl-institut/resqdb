WITH storages AS (
    SELECT
        scenario_id,
        scenario.name AS scenario_name,
        from_node,
        to_node,
        CASE
            WHEN from_node LIKE ANY(Array['%storage%', '%battery%']) THEN CONCAT(from_label.label, ' (Entladen)')
            ELSE from_label.label
        END AS from_label,
        CASE
            WHEN to_node LIKE ANY(Array['%storage%', '%battery%']) THEN CONCAT(to_label.label, ' (Laden)')
            ELSE to_label.label
        END AS to_label,
        category.category,
        total_energy
    FROM sequence
    JOIN scenario ON sequence.scenario_id = scenario.id
    JOIN label AS from_label ON sequence.from_node = from_label.component
    JOIN label AS to_label ON sequence.to_node = to_label.component
    LEFT JOIN category USING(from_node, to_node)
    WHERE sequence.attribute = 'flow'
)
SELECT
    *
FROM storages
-- SELECT
--     scenario_id,
--     scenario.name AS scenario_name,
--     from_node,
--     to_node,
--     label.label,
--     category.category,
--     total_energy
-- FROM sequence
-- JOIN scenario ON sequence.scenario_id = scenario.id
-- LEFT JOIN label USING(from_node, to_node)
-- LEFT JOIN category USING(from_node, to_node)
-- WHERE sequence.attribute = 'flow' and to_node NOT LIKE '%storage%' and from_node NOT LIKE '%conn%' and to_node NOT LIKE '%conn%'
-- ORDER BY scenario_id
