import pytest

from scripts.ops.maintain_table_indexes import resolve_table_role


@pytest.mark.parametrize(
    ("table_name", "expected_role"),
    [
        ("wind_tunnel_landing", "landing"),
        ("v2_landing_test", "landing"),
        ("wind_tunnel_serving", "serving"),
        ("serving_test", "serving"),
    ],
)
def test_resolve_table_role_for_standard_tables(table_name, expected_role):
    assert resolve_table_role(table_name) == expected_role


def test_resolve_table_role_rejects_custom_table():
    with pytest.raises(ValueError, match="Unsupported table"):
        resolve_table_role("custom_trajectory_table")
