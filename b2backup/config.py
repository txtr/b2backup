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
        ValueError: If YAML is malformed or missing required fields.
    """
    with open(path) as f:
        data = yaml.safe_load(f)

    if not data or "accounts" not in data:
        raise ValueError("Config must contain 'accounts' list")

    accounts = []
    for i, a in enumerate(data["accounts"]):
        if "account_id" not in a or "account_key" not in a:
            raise ValueError(f"Account {i} missing required fields (account_id, account_key)")
        accounts.append(Account(
            account_id=a["account_id"],
            account_key=a["account_key"],
            capacity_in_mb=a.get("capacity_in_mb"),
        ))

    if not accounts:
        raise ValueError("At least one account is required")

    return accounts