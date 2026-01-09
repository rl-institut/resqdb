"""Test scenarios.py."""

import pytest
from scenarios import get_cluster_for_component
from settings import COMPONENT_CLUSTERS


COMPONENT_CLUSTERS["test"] = "non-existing"


def test_get_cluster_for_component_raises_if_cluster_missing_in_db() -> None:
    """Component "wind" maps to cluster name "Bürogebäude"; simulate missing in DB."""
    with pytest.raises(KeyError) as exc:
        get_cluster_for_component("test")

    # Helpful error contains both the cluster and component names
    msg = str(exc.value)
    assert "test" in msg
    assert "non-existing" in msg


def test_get_cluster_for_component_returns_none_if_component_unknown() -> None:
    """Component "unknown" is unknown to the database."""
    assert get_cluster_for_component("unknown") is None


def test_get_cluster_for_component() -> None:
    """Get cluster ID for an existing component."""
    assert get_cluster_for_component("wind") is not None
