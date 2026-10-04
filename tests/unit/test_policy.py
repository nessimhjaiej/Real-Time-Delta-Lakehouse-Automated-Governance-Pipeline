from pathlib import Path

import pytest
import yaml

from src.governance_agent.exceptions import UnknownRoleError
from src.governance_agent.policy import AccessPolicy, RolePolicy, load_access_policy


def test_loads_the_real_access_policy() -> None:
    policy = load_access_policy()
    analyst = policy.get_role("analyst")

    assert set(analyst.allowed_tables) == {
        "fact_orders",
        "dim_customers",
        "dim_products",
    }
    assert analyst.max_rows > 0
    assert analyst.allowed_operations == ["SELECT"]


def test_real_policy_blocks_pii_on_both_customer_id_columns() -> None:
    # fact_orders.customer_id and dim_customers.customer_id are the exact
    # same unsalted hash (see Stage 1 findings) -- both must be PII.
    analyst = load_access_policy().get_role("analyst")

    assert analyst.is_pii_column("fact_orders", "customer_id")
    assert analyst.is_pii_column("dim_customers", "customer_id")
    assert analyst.is_pii_column("dim_customers", "full_name")
    assert analyst.is_pii_column("dim_customers", "email")
    assert analyst.is_pii_column("dim_customers", "ip_address")
    assert not analyst.is_pii_column("dim_customers", "country")


def test_loader_reads_a_custom_policy_file(tmp_path: Path) -> None:
    path = tmp_path / "access.yml"
    path.write_text(
        yaml.dump(
            {
                "roles": {
                    "viewer": {
                        "description": "d",
                        "allowed_tables": ["t"],
                        "allowed_columns": {"t": ["c"]},
                        "max_rows": 50,
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    policy = load_access_policy(path)
    role = policy.get_role("viewer")

    assert role.max_rows == 50
    assert role.allowed_operations == ["SELECT"]  # default
    assert role.pii_columns == {}  # default


def test_unknown_role_raises_typed_error() -> None:
    policy = AccessPolicy(roles={})

    with pytest.raises(UnknownRoleError) as excinfo:
        policy.get_role("nope")
    assert excinfo.value.name == "nope"


def test_is_allowed_column_and_is_pii_column_are_independent() -> None:
    role = RolePolicy(
        description="d",
        allowed_tables=["t"],
        allowed_columns={"t": ["id"]},
        pii_columns={"t": ["id"]},
        max_rows=10,
    )

    # id is both "allowed" (it's a valid join key) and PII (must never be
    # selected/filtered) -- the two checks are independent on purpose.
    assert role.is_allowed_column("t", "id")
    assert role.is_pii_column("t", "id")
    assert not role.is_allowed_column("t", "unknown")
    assert not role.is_pii_column("t", "unknown")
