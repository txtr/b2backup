"""Configuration loading for b2backup."""
import yaml
from dataclasses import dataclass
from typing import Optional


@dataclass
class Account:
    """B2 account configuration from accounts.yaml."""
    account_id: str
    account_key: str
    capacity_in_mb: Optional[int] = None


def load_config(path: str) -> list[Account]:
    """Load accounts from YAML config file.

    Args:
        path: Path to accounts.yaml file.

    Returns:
        List of Account objects.

    Raises:
        FileNotFoundError: If config file doesn't exist.
        ValueError: If YAML is malformed or contains invalid values.
    """
    with open(path) as f:
        try:
            data = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML: {e}") from e

    if not isinstance(data, dict) or "accounts" not in data:
        raise ValueError("Config must contain top-level 'accounts' list")

    raw_accounts = data["accounts"]
    if not isinstance(raw_accounts, list):
        raise ValueError("'accounts' must be a list")

    accounts = []
    seen_ids: set[str] = set()
    for i, a in enumerate(raw_accounts):
        if not isinstance(a, dict):
            raise ValueError(f"Account {i} must be a mapping")

        account_id = a.get("account_id")
        account_key = a.get("account_key")
        if not isinstance(account_id, str) or not account_id.strip():
            raise ValueError(f"Account {i}: 'account_id' must be a non-empty string")
        if not isinstance(account_key, str) or not account_key.strip():
            raise ValueError(f"Account {i}: 'account_key' must be a non-empty string")

        capacity = a.get("capacity_in_mb")
        if capacity is not None:
            if isinstance(capacity, bool) or not isinstance(capacity, int) or capacity < 0:
                raise ValueError(
                    f"Account {i}: 'capacity_in_mb' must be a non-negative integer or omitted"
                )

        if account_id in seen_ids:
            raise ValueError(f"Duplicate account_id '{account_id}'")
        seen_ids.add(account_id)

        accounts.append(Account(
            account_id=account_id,
            account_key=account_key,
            capacity_in_mb=capacity,
        ))

    if not accounts:
        raise ValueError("At least one account is required")

    return accounts