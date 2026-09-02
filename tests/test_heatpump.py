"""Test heatpump.py."""

import shutil
import pytest
import pandas as pd

from collections.abc import Sequence
from typing import Union
from oemof.solph import Bus, EnergySystem, Flow, Model, components, processing, views
from facades.heatpump import AquiferHeatpump

requires_solver = pytest.mark.skipif(
    shutil.which("cbc") is None,
    reason="cbc solver not installed",
)


def test_cop_below_one_raises() -> None:
    """A COP of 1 or less would draw no heat from the aquifer and is rejected on construction."""
    # electricity and heat
    baq = Bus(label="aquifer_bus")
    bel = Bus(label="electricity_bus")
    bth = Bus(label="heat_bus")

    with pytest.raises(ValueError, match=r"COP of 'aquifer_hp' must be greater than 1"):
        AquiferHeatpump(
            label="aquifer_hp",
            carrier="electricity",
            tech="hp",
            aquifer_bus=baq,
            electricity_bus=bel,
            heat_bus=bth,
            cop=1.0,
        )


def build_energysystem(
    cop: Union[float, Sequence[float]],
    demand: Sequence[float],
    *,
    capacity: float | None = None,
    expandable: bool = False,
    capacity_cost: float | None = None,
) -> EnergySystem:
    """
    Build a minimal energy system around a single AquiferHeatpump.

    Topology::

        electricity (Source) -> electricity_bus -+
                                                 +-> aquifer_hp -+
        aquifer (Source)     -> aquifer_bus    --+               |
                                                                 v
                        additional_heat_source (Source) -> heat_bus -> heat_demand (Sink)

    The electricity and aquifer sources are unbounded, so the fixed heat demand
    alone determines every flow in the system.

    ``additional_heat_source`` is a deliberately expensive backup on the heat bus.
    It keeps the model feasible when the heatpump is too small to cover the demand
    and stays at zero otherwise, so it does not disturb the unbounded cases.

    Parameters
    ----------
    cop : float or sequence of float
        Coefficient of performance, either scalar or one value per time step.
    demand : sequence of float
        Heat demand per time step, in absolute units. The sink is built with
        ``nominal_value=1``, so these values are used as they are instead of
        being scaled as a normed profile. Its length sets the number of time
        steps.
    capacity : float or None
        Electrical capacity of the heatpump. None leaves the heatpump
        unbounded. When ``expandable`` is True this is the existing capacity
        instead, and None is then not allowed - see
        test_investment_without_capacity_raises.
    expandable : bool
        Whether the electrical capacity is an investment variable rather than a
        fixed bound. Requires ``capacity_cost``.
    capacity_cost : float or None
        Investment cost per unit of electrical capacity, in currency per MW_el.
        Only read when ``expandable`` is True.

    Returns
    -------
    EnergySystem
        The assembled energy system, not yet solved.

    """
    timeindex = pd.date_range("2025-01-01", periods=len(demand), freq="h")
    es = EnergySystem(timeindex=timeindex, infer_last_interval=True)

    baq = Bus(label="aquifer_bus")
    bel = Bus(label="electricity_bus")
    bth = Bus(label="heat_bus")

    el_import = components.Source(
        label="electricity",
        outputs={bel: Flow(variable_costs=1)},
    )
    aquifer_source = components.Source(label="aquifer", outputs={baq: Flow()})
    heat_demand = components.Sink(
        label="heat_demand",
        inputs={bth: Flow(nominal_value=1, fix=demand)},
    )
    additonal_heat_source = components.Source(
        label="additional_heat_source",
        outputs={bth: Flow(variable_costs=1000)},
    )

    aquifer_hp = AquiferHeatpump(
        label="aquifer_hp",
        carrier="electricity",
        tech="hp",
        aquifer_bus=baq,
        electricity_bus=bel,
        heat_bus=bth,
        cop=cop,
        capacity=capacity,
        expandable=expandable,
        capacity_cost=capacity_cost,
    )

    es.add(
        baq,
        bel,
        bth,
        el_import,
        aquifer_source,
        heat_demand,
        aquifer_hp,
        additonal_heat_source,
    )

    return es


def solve_energysystem(es: EnergySystem) -> dict:
    """
    Solve an energy system with cbc and return its flow results.

    Fails the calling test if the solver does not reach an optimal solution, so
    that later assertions cannot silently compare against missing values.

    Parameters
    ----------
    es : EnergySystem
        The energy system to solve.

    Returns
    -------
    dict
        Results keyed by label tuples such as ``("electricity_bus", "aquifer_hp")``
        rather than by node objects. The last time point is dropped, so every
        flow series holds exactly one value per time step.

    """
    m = Model(es)
    solver_results = m.solve(solver="cbc")
    assert solver_results["Solver"][0]["Termination condition"] == "optimal"

    results = processing.results(m, remove_last_time_point=True)

    return views.convert_keys_to_strings(results)


@requires_solver
def test_scalar_cop_couples_all_three_flows() -> None:
    """
    A scalar COP fixes all three flows of the heatpump relative to each other.

    Because the heat demand is imposed as a fixed profile, the solution is unique
    and the expected values can be derived by hand instead of from the formula
    under test: 40 MWh of heat at a COP of 4 needs 10 MWh of electricity and
    draws the remaining 30 MWh from the aquifer.

    The last assertion states the underlying energy balance explicitly. It is
    redundant for these particular numbers, but it records why this triple and no
    other one is the correct answer.
    """
    results = solve_energysystem(build_energysystem(cop=4.0, demand=[40, 40, 40]))
    el = results[("electricity_bus", "aquifer_hp")]["sequences"]["flow"]
    aquifer = results[("aquifer_bus", "aquifer_hp")]["sequences"]["flow"]
    bth = results[("aquifer_hp", "heat_bus")]["sequences"]["flow"]

    assert list(el) == pytest.approx([10, 10, 10])
    assert list(aquifer) == pytest.approx([30, 30, 30])
    assert list(bth) == pytest.approx([40, 40, 40])
    assert list(el + aquifer) == pytest.approx(list(bth))


@requires_solver
def test_sequence_cop_couples_all_three_flows() -> None:
    """
    A COP given as a time series is applied per time step.

    The heat demand is imposed as a fixed profile, so every flow follows from the
    COP of its own time step. Derived by hand, not from the formula under test:
    40 MWh at a COP of 4 needs 10 MWh of electricity and 30 MWh from the aquifer,
    30 MWh at a COP of 3 needs 10 and 20, and 40 MWh at a COP of 2 needs 20 and 20.

    That the expected values differ between time steps is the point of this test.
    Were the list collapsed to its first entry, the 30 MWh of the second time step
    would be met with 7.5 MWh of electricity instead of 10.

    The last assertion states the underlying energy balance explicitly, as in
    test_scalar_cop_couples_all_three_flows.
    """
    results = solve_energysystem(
        build_energysystem(cop=[4.0, 3.0, 2.0], demand=[40, 30, 40]),
    )
    el = results[("electricity_bus", "aquifer_hp")]["sequences"]["flow"]
    aquifer = results[("aquifer_bus", "aquifer_hp")]["sequences"]["flow"]
    bth = results[("aquifer_hp", "heat_bus")]["sequences"]["flow"]

    assert list(el) == pytest.approx([10, 10, 20])
    assert list(aquifer) == pytest.approx([30, 20, 20])
    assert list(bth) == pytest.approx([40, 30, 40])
    assert list(el + aquifer) == pytest.approx(list(bth))


@requires_solver
def test_investment_is_electrical() -> None:
    """
    Investment is attached to the electricity input, so it is sized in MW_el.

    Covering 40 MWh of heat at a COP of 4 needs 10 MW_el, and that is what the
    optimizer invests in. Were the investment attached to the heat output, the
    result would be 40 - the difference is exactly the COP.

    This also fixes the unit of ``capacity_cost``: currency per MW_el, not per
    MW_th. Parametrising it with thermal costs would leave the model solvable and
    merely scale the investment part of the objective by the COP, which is the
    kind of error no solver reports.

    ``capacity=0`` is required rather than cosmetic, see
    test_investment_without_capacity_raises.
    """
    results = solve_energysystem(
        build_energysystem(
            cop=4.0,
            demand=[40, 40, 40],
            expandable=True,
            capacity_cost=100,
            capacity=0,
        ),
    )
    assert results[("electricity_bus", "aquifer_hp")]["scalars"]["invest"] == pytest.approx(10)


@requires_solver
def test_capacity_limits_electricity_input() -> None:
    """
    A fixed capacity bounds the electricity input, not the heat output.

    At 5 MW_el and a COP of 4 the heatpump delivers 20 MWh of heat; the remaining
    20 MWh of the 40 MWh demand come from the expensive backup source. Were the
    bound on the heat output, the heatpump would be capped at 5 MWh of heat.

    The assertion on the electricity flow is the discriminating one, because that
    is the flow the bound is attached to.
    """
    results = solve_energysystem(
        build_energysystem(cop=4.0, demand=[40, 40, 40], capacity=5),
    )
    el = results[("electricity_bus", "aquifer_hp")]["sequences"]["flow"]
    bth = results[("aquifer_hp", "heat_bus")]["sequences"]["flow"]
    bheat_additional = results[("additional_heat_source", "heat_bus")]["sequences"]["flow"]

    assert list(el) == pytest.approx([5, 5, 5])
    assert list(bth) == pytest.approx([20, 20, 20])
    assert list(bheat_additional) == pytest.approx([20, 20, 20])


def test_investment_without_capacity_raises() -> None:
    """
    Investment without an explicit capacity crashes, which is pinned here.

    ``Facade._investment()`` passes ``existing=getattr(self, "capacity", 0)``, and
    that default never applies: ``capacity`` exists as a dataclass field holding
    None, so ``Investment.existing`` becomes None and solph adds it to a decision
    variable. A getattr default guards against a missing attribute, not against
    an attribute that is present and None.

    The failure surfaces while the model is built, not while the energy system is
    assembled, which is why ``Model`` is the call under test here. The workaround
    is ``capacity=0`` for a greenfield investment. This is not specific to
    AquiferHeatpump: every oemof.tabular facade except Storage defaults
    ``capacity`` to None.

    This test pins a defect, not desired behaviour. Turning red is the wanted
    signal that the bug has been fixed upstream, and it should be deleted then.
    """
    es = build_energysystem(
        cop=4.0,
        demand=[40, 40, 40],
        expandable=True,
        capacity_cost=100,
    )

    with pytest.raises(TypeError, match=r"'VarData' and 'NoneType'"):
        Model(es)
