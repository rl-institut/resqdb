WITH
  renewables AS (
    SELECT
      scenario_id,
      from_node,
      to_node,
      label.label,
      category,
      total_energy,
      timeseries
    FROM
      sequence
      LEFT JOIN label USING (from_node, to_node)
      LEFT JOIN category USING (from_node, to_node)
    WHERE
      attribute = 'flow'
      AND carrier = 'electricity'
      AND (
        is_renewable
        OR from_node LIKE '%battery%'
        OR category = 'Verbrauch'
      )
    ORDER BY
      scenario_id
  ),
  autarky_per_timestep AS (
    SELECT
      scenario_id,
      timestep,
      SUM(
        CASE
          WHEN category = 'Verbrauch' THEN - elem
          ELSE elem
        END
      ) AS value
    FROM
      renewables,
      unnest(timeseries) WITH ORDINALITY a (elem, timestep)
    GROUP BY
      scenario_id,
      timestep
  )
SELECT
  scenario_id,
  'Zeitgleich' AS type,
  (
    COUNT(*) FILTER (
      WHERE
        value > 0
    )
  )::DECIMAL / 8760 * 100 AS autarky
FROM
  autarky_per_timestep
GROUP BY
  scenario_id
UNION
SELECT
  scenario_id,
  'Bilanziell' AS type,
  SUM(total_energy) FILTER (
    WHERE
      category = 'Erzeugung'
  ) / SUM(total_energy) FILTER (
    WHERE
      category = 'Verbrauch'
  ) * 100 AS autarky
FROM
  renewables
GROUP BY
  scenario_id
