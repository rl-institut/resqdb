WITH
  renewables AS (
    SELECT
      sequence.id AS sequence_id,
      scenario_id,
      scenario.name AS scenario_name,
      category
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
  ),
  autarky_per_timestep AS (
    SELECT
      scenario_id,
      scenario_name,
      value
    FROM renewables
    JOIN timeseries USING (sequence_id)
  )
SELECT
  scenario_id,
  scenario_name,
  'Zeitgleich' AS type,
  COUNT(*) FILTER (
    WHERE
      value > 0
  ) AS autarky
FROM
  autarky_per_timestep
GROUP BY
  scenario_id,
  scenario_name
