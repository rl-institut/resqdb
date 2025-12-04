
import pytest
from scenarios import COMPONENT_CLUSTERS, get_cluster_for_component


COMPONENT_CLUSTERS["test"] = "non-existing"


def test_get_cluster_for_component_raises_if_cluster_missing_in_db():
    """Component "wind" maps to cluster name "Bürogebäude"; simulate missing in DB"""

    with pytest.raises(KeyError) as exc:
        get_cluster_for_component("test")

    # Helpful error contains both the cluster and component names
    msg = str(exc.value)
    assert "test" in msg and "non-existing" in msg


def test_get_cluster_for_component_returns_none_if_component_unknown():
    """Component "unknown" is unknown to the database"""
    assert get_cluster_for_component("unknown") is None


def test_get_cluster_for_component():
    """Get cluster ID for existing component."""
    assert get_cluster_for_component("wind") is not None
