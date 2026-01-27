WITH
  renewables AS (
    SELECT
      scenario_id,
      scenario.name AS scenario_name,
      category,
      total_energy,
      timeseries
    FROM
      sequence
      JOIN scenario ON sequence.scenario_id = scenario.id
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
  )
SELECT
  scenario_id,
  scenario_name,
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
  scenario_id,
  scenario_name
