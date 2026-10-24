"""Core B2 storage logic: account pool, cache, routing, and mutations."""
import itertools
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from b2sdk.v2 import B2Api, InMemoryAccountInfo, Bucket
from .config import Account

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

B2_MAX_FILES_PER_ACCOUNT = 1700


@dataclass
class AccountState:
    """Runtime state for a single B2 account/bucket."""
    account: Account
    bucket_name: str
    bucket: Bucket
    used_bytes: int = 0
    file_count: int = 0


class B2AccountPool:
    """Manages a pool of B2 accounts with unified file namespace.

    Startup:
        1. Authorize each account
        2. Discover bucket (must be exactly one per account)
        3. Scan all buckets (latest_only=True) to build cache
        4. Validate no duplicate filenames across accounts
        5. Compute used_bytes and file_count per account

    Invariants:
        - _cache[name] = owning AccountState for every file in latest versions
        - No filename exists in more than one account's latest versions
        - used_bytes/file_count reflect latest versions only
    """

    def __init__(self, accounts: list[Account]):
        self._states: list[AccountState] = []
        self._cache: dict[str, AccountState] = {}  # filename -> owning AccountState
        self._cycle = itertools.cycle(range(len(accounts)))

        # Phase 1: Authorize and discover buckets
        for acc in accounts:
            api = B2Api(InMemoryAccountInfo())
            api.authorize_account("production", acc.account_id, acc.account_key)

            buckets = api.list_buckets()
            if len(buckets) != 1:
                log.warning(
                    "Skipping account %s: must expose exactly one bucket (found %d).",
                    acc.account_id, len(buckets)
                )
                continue

            bucket = buckets[0]
            state = AccountState(
                account=acc,
                bucket_name=bucket.name,
                bucket=bucket,
            )
            self._states.append(state)

        if not self._states:
            raise RuntimeError("No valid accounts with exactly one bucket found")

        # Phase 2: Build cache + validate consistency + compute usage
        self._build_cache()

        log.info("Pool ready: %d accounts, %d unique files", len(self._states), len(self._cache))
        for s in self._states:
            rem_bytes = self._remaining_bytes(s)
            rem_files = self._remaining_files(s)
            cap_str = f"{rem_bytes / 1024 / 1024:.0f}MB" if rem_bytes is not None else "unlimited"
            log.info(
                "  %s (%s): %d files, %.0fMB used, remaining: %s, %d slots",
                s.account.account_id[:8], s.bucket_name,
                s.file_count, s.used_bytes / 1024 / 1024,
                cap_str, rem_files
            )

    def _build_cache(self) -> None:
        """Scan all buckets, detect duplicates, compute usage."""
        for state in self._states:
            for f in state.bucket.ls(latest_only=True):
                name = f.file_name
                size = f.size

                # Track usage
                state.used_bytes += size
                state.file_count += 1

                # Check duplicate across accounts
                if name in self._cache:
                    other = self._cache[name]
                    raise RuntimeError(
                        f"CONSISTENCY VIOLATION: '{name}' exists in both "
                        f"'{other.bucket_name}' and '{state.bucket_name}'. "
                        f"Run b2router-reconcile --apply first."
                    )
                self._cache[name] = state

    def _remaining_bytes(self, state: AccountState) -> Optional[int]:
        """Bytes remaining in this account's soft capacity budget."""
        if state.account.capacity_in_mb is None:
            return None
        cap = state.account.capacity_in_mb * 1024 * 1024
        return max(0, cap - state.used_bytes)

    def _remaining_files(self, state: AccountState) -> int:
        """File slots remaining (B2 hard limit 1700)."""
        return max(0, B2_MAX_FILES_PER_ACCOUNT - state.file_count)

    def _eligible_accounts(
        self,
        file_size: int,
        exclude: Optional[AccountState] = None
    ) -> list[AccountState]:
        """Accounts that can fit this file (bytes + slots), optionally excluding one."""
        eligible = []
        for state in self._states:
            if state is exclude:
                continue
            rem_bytes = self._remaining_bytes(state)
            if rem_bytes is not None and rem_bytes < file_size:
                continue
            if self._remaining_files(state) <= 0:
                continue
            eligible.append(state)
        return eligible

    def _pick_account(
        self,
        file_size: int,
        exclude: Optional[AccountState] = None
    ) -> AccountState:
        """Best-fit: account with smallest remaining bytes after placement."""
        eligible = self._eligible_accounts(file_size, exclude)
        if not eligible:
            raise RuntimeError("No account has space/slots for this file")

        # Best fit: smallest remaining_after
        def remaining_after(s: AccountState) -> int:
            rem = self._remaining_bytes(s)
            return (rem - file_size) if rem is not None else 10**18

        # Tie-break: account_id for determinism
        return min(eligible, key=lambda s: (remaining_after(s), s.account.account_id))

    def upload_file(self, name: str, local_path: Path) -> None:
        """Upload or replace a file.

        New file: pick best-fit account.
        Replacement: pick best-fit account EXCLUDING current owner.
        Atomic-ish: new version uploaded before old deleted.
        """
        file_size = local_path.stat().st_size
        existing_state = self._cache.get(name)

        if existing_state is None:
            # New file
            state = self._pick_account(file_size)
            data = local_path.read_bytes()
            state.bucket.upload_bytes(data, name)
            state.used_bytes += file_size
            state.file_count += 1
            self._cache[name] = state
            log.info("Created %s in %s (%.1fKB)", name, state.bucket_name, file_size / 1024)
        else:
            # Replacement: must use DIFFERENT account
            state = self._pick_account(file_size, exclude=existing_state)
            data = local_path.read_bytes()
            state.bucket.upload_bytes(data, name)
            state.used_bytes += file_size
            state.file_count += 1

            # Update cache to new location
            self._cache[name] = state

            # Best-effort delete old
            try:
                old_file = existing_state.bucket.get_file_info_by_name(name)
                existing_state.bucket.delete_file_version(old_file.id_, name)
                existing_state.used_bytes -= old_file.size
                existing_state.file_count -= 1
                log.info("Replaced %s: %s -> %s", name, existing_state.bucket_name, state.bucket_name)
            except Exception as e:
                log.warning("Replaced %s but failed to delete old version: %s", name, e)

    def download_file(self, name: str, dest_path: Path) -> None:
        """Download a file by its stored name."""
        state = self._cache.get(name)
        if not state:
            raise FileNotFoundError(name)

        file_obj = state.bucket.get_file_info_by_name(name)
        content = state.bucket.download_file_by_id(file_obj.id_).response.content
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        dest_path.write_bytes(content)

    def delete_file(self, name: str) -> None:
        """Delete a file by its stored name."""
        state = self._cache.get(name)
        if not state:
            raise FileNotFoundError(name)

        file_obj = state.bucket.get_file_info_by_name(name)
        state.bucket.delete_file_version(file_obj.id_, name)
        state.used_bytes -= file_obj.size
        state.file_count -= 1
        del self._cache[name]
        log.info("Deleted %s from %s", name, state.bucket_name)

    def list_files(self) -> list[dict]:
        """List all files with metadata (live B2 lookup for freshness)."""
        result = []
        for name, state in sorted(self._cache.items()):
            file_obj = state.bucket.get_file_info_by_name(name)
            result.append({
                "name": name,
                "size": file_obj.size,
                "upload_timestamp": file_obj.upload_timestamp,
                "content_type": file_obj.content_type,
                "account": state.account.account_id[:8],
            })
        return result

    def get_pool_stats(self) -> dict:
        """Get pool capacity and usage statistics."""
        total_files = sum(s.file_count for s in self._states)
        total_bytes = sum(s.used_bytes for s in self._states)
        return {
            "accounts": len(self._states),
            "total_files": total_files,
            "total_bytes": total_bytes,
            "per_account": [
                {
                    "account_id": s.account.account_id[:8],
                    "bucket": s.bucket_name,
                    "files": s.file_count,
                    "used_mb": round(s.used_bytes / 1024 / 1024, 1),
                    "remaining_files": self._remaining_files(s),
                    "remaining_mb": round(self._remaining_bytes(s) / 1024 / 1024, 1)
                        if self._remaining_bytes(s) is not None else "unlimited",
                }
                for s in self._states
            ]
        }