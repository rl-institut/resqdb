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
  ),
  autarky_per_timestep AS (
    SELECT
      scenario_id,
      scenario_name,
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
      scenario_name,
      timestep
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
