"""Loads policies/access.yml: role -> allowed tables/columns/PII/limits.
GOVERENENCE IS HARD CODED  !
validator.py uses the policies loaded here to validate queries  .
"""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel

from src.governance_agent.exceptions import UnknownRoleError

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_POLICY_PATH = PROJECT_ROOT / "policies" / "access.yml"


class RolePolicy(BaseModel):
    description: str
    allowed_tables: list[str]
    allowed_columns: dict[str, list[str]]
    pii_columns: dict[str, list[str]] = {}
    max_rows: int
    allowed_operations: list[str] = ["SELECT"]

    def is_pii_column(self, table: str, column: str) -> bool:
        return column in self.pii_columns.get(table, [])

    def is_allowed_column(self, table: str, column: str) -> bool:
        return column in self.allowed_columns.get(table, [])


class AccessPolicy(BaseModel):
    roles: dict[str, RolePolicy]

    def get_role(self, name: str) -> RolePolicy:
        try:
            return self.roles[name]
        except KeyError:
            raise UnknownRoleError(name) from None


def load_access_policy(path: Path = DEFAULT_POLICY_PATH) -> AccessPolicy:
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    roles = {name: RolePolicy(**raw) for name, raw in data.get("roles", {}).items()}
    return AccessPolicy(roles=roles)
