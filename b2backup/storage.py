"""Core B2 storage logic: account pool, cache, routing, and mutations."""
import itertools
import logging
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from b2sdk.v2 import B2Api, InMemoryAccountInfo, Bucket
from .config import Account

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

B2_MAX_FILES_PER_ACCOUNT = 1700


class BucketValidationError(Exception):
    """Raised when an account has != 1 bucket."""
    def __init__(self, issues: list[tuple[Account, int, list[str]]]):
        self.issues = issues  # (account, bucket_count, bucket_names)
        super().__init__(self._format_message())

    def _format_message(self) -> str:
        lines = ["ERROR: Account validation failed"]
        for acc, count, names in self.issues:
            bucket_list = ", ".join(names) if names else "(none)"
            lines.append(f"  {acc.account_id}: {count} buckets (expected 1) - {bucket_list}")
        lines.append("Hint: Each application key must be restricted to exactly one bucket in B2 console.")
        return "\n".join(lines)


class ConsistencyError(Exception):
    """Raised when consistency checks fail."""
    def __init__(self, message: str, details: list[str]):
        self.message = message
        self.details = details
        super().__init__(self._format_message())

    def _format_message(self) -> str:
        lines = [f"ERROR: {self.message}"]
        lines.extend(f"  {d}" for d in self.details)
        return "\n".join(lines)


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

    Startup (strict validation, no mutations allowed if inconsistent):
        1. Authorize each account
        2. Discover buckets - MUST be exactly one per account
        3. Scan all buckets (latest_only=False) to:
           - Compute true used_bytes/file_count (all versions count)
           - Detect multiple versions of same file within account
           - Build latest-version cache for cross-account duplicate detection
        4. Validate no duplicate filenames across accounts (latest only)
        5. Log pool summary

    Invariants (enforced at startup, maintained on mutations):
        - _cache[name] = owning AccountState for every file's LATEST version
        - No filename exists in more than one account's latest versions
        - used_bytes/file_count reflect ALL versions (for capacity tracking)
    """

    def __init__(self, accounts: list[Account]):
        self._states: list[AccountState] = []
        self._cache: dict[str, AccountState] = {}  # filename -> owning AccountState (latest)
        self._cycle = itertools.cycle(range(len(accounts)))

        # Phase 1: Authorize and discover buckets
        account_buckets: list[tuple[Account, list[Bucket]]] = []
        for acc in accounts:
            api = B2Api(InMemoryAccountInfo())
            api.authorize_account("production", acc.account_id, acc.account_key)
            buckets = api.list_buckets()
            account_buckets.append((acc, buckets))

        # Phase 2: Validate bucket counts (must be exactly 1)
        bucket_issues = []
        for acc, buckets in account_buckets:
            if len(buckets) != 1:
                bucket_issues.append((acc, len(buckets), [b.name for b in buckets]))

        if bucket_issues:
            raise BucketValidationError(bucket_issues)

        # All accounts valid - create states
        for acc, buckets in account_buckets:
            bucket = buckets[0]
            state = AccountState(
                account=acc,
                bucket_name=bucket.name,
                bucket=bucket,
            )
            self._states.append(state)

        # Phase 3: Build cache + validate consistency + compute usage
        self._build_cache_and_validate()

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

    def _build_cache_and_validate(self) -> None:
        """Scan all buckets (all versions), validate consistency, compute usage."""
        cross_duplicates: dict[str, list[AccountState]] = defaultdict(list)
        intra_duplicates: dict[str, dict[str, list]] = defaultdict(dict)  # account_id -> filename -> versions

        for state in self._states:
            versions_per_file: dict[str, list] = defaultdict(list)

            # Scan ALL versions for true capacity tracking
            for f in state.bucket.ls(latest_only=False):
                state.used_bytes += f.size
                state.file_count += 1
                versions_per_file[f.file_name].append(f)

            # Check for multiple versions within this account
            for name, versions in versions_per_file.items():
                if len(versions) > 1:
                    # Store for error reporting
                    if state.account.account_id not in intra_duplicates:
                        intra_duplicates[state.account.account_id] = {}
                    intra_duplicates[state.account.account_id][name] = versions

                # Pick latest for cache
                latest = max(versions, key=lambda v: v.upload_timestamp)
                if name in self._cache:
                    cross_duplicates[name].append(state)
                else:
                    self._cache[name] = state

        # Phase 4: Report intra-account multiple versions
        if intra_duplicates:
            details = []
            for acc_id, files in intra_duplicates.items():
                bucket_name = next(s.bucket_name for s in self._states if s.account.account_id == acc_id)
                details.append(f"Account {acc_id} ({bucket_name}):")
                for name, versions in files.items():
                    total_size = sum(v.size for v in versions)
                    details.append(
                        f"    {name} has {len(versions)} versions "
                        f"(occupying {total_size / 1024 / 1024:.1f}MB, {len(versions)} slots)"
                    )
            details.append("Hint: Delete old versions manually in B2 console or via B2 CLI.")
            raise ConsistencyError(
                "Consistency check failed - multiple versions in single account",
                details
            )

        # Phase 5: Report cross-account duplicate filenames (latest only)
        if cross_duplicates:
            details = []
            for name, states in cross_duplicates.items():
                details.append(f"{name} exists in:")
                for s in states:
                    latest = s.bucket.get_file_info_by_name(name)
                    ts = latest.upload_timestamp
                    details.append(f"  - {s.account.account_id[:8]} ({s.bucket_name}) [latest: {ts}]")
            details.append("Hint: Delete duplicate from all but one account manually.")
            raise ConsistencyError(
                "Consistency check failed - duplicate filenames across accounts",
                details
            )

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

    def _get_eligible_accounts_sorted(
        self,
        file_size: int,
        exclude: Optional[AccountState] = None
    ) -> list[AccountState]:
        """Get eligible accounts sorted by best-fit (best first)."""
        eligible = self._eligible_accounts(file_size, exclude)
        if not eligible:
            return []

        def remaining_after(s: AccountState) -> int:
            rem = self._remaining_bytes(s)
            return (rem - file_size) if rem is not None else 10**18

        return sorted(eligible, key=lambda s: (remaining_after(s), s.account.account_id))

    def _pick_account(
        self,
        file_size: int,
        exclude: Optional[AccountState] = None
    ) -> AccountState:
        """Best-fit: account with smallest remaining bytes after placement.
        
        Returns the single best account (first in sorted eligible list).
        Raises if no eligible accounts.
        """
        eligible = self._get_eligible_accounts_sorted(file_size, exclude)
        if not eligible:
            raise RuntimeError("No account has space/slots for this file")
        return eligible[0]

    def _try_upload_to_accounts(
        self,
        name: str,
        data: bytes,
        file_size: int,
        eligible_accounts: list[AccountState],
        action: str
    ) -> Optional[AccountState]:
        """Try uploading to each eligible account in order. Returns successful state or None."""
        last_error = None
        for state in eligible_accounts:
            try:
                state.bucket.upload_bytes(data, name)
                state.used_bytes += file_size
                state.file_count += 1
                log.info("%s %s in %s (%.1fKB)", action, name, state.bucket_name, file_size / 1024)
                return state
            except Exception as e:
                last_error = e
                log.warning("Failed to %s %s to %s: %s; trying next account", action.lower(), name, state.bucket_name, e)
                continue
        
        log.error("All %d eligible accounts failed for %s. Last error: %s. Skipping file.", 
                  len(eligible_accounts), name, last_error)
        return None

    def upload_file(self, name: str, local_path: Path) -> None:
        """Upload or replace a file with failover to next eligible account.

        New file: try best-fit accounts in order until one succeeds.
        Replacement: try best-fit accounts (excluding current owner) in order.
        If all accounts fail: log error and skip file (continue with next).
        """
        file_size = local_path.stat().st_size
        existing_state = self._cache.get(name)
        data = local_path.read_bytes()

        if existing_state is None:
            # New file: try all eligible accounts
            eligible = self._get_eligible_accounts_sorted(file_size)
            if not eligible:
                log.error("No eligible account has space/slots for %s (size=%.1fKB). Skipping.", 
                          name, file_size / 1024)
                return

            state = self._try_upload_to_accounts(name, data, file_size, eligible, "Created")
            if state:
                self._cache[name] = state
            # If None, error already logged, skip file
        else:
            # Replacement: must use DIFFERENT account
            eligible = self._get_eligible_accounts_sorted(file_size, exclude=existing_state)
            if not eligible:
                log.error("No eligible account (excluding current owner) has space/slots for %s (size=%.1fKB). Skipping.", 
                          name, file_size / 1024)
                return

            state = self._try_upload_to_accounts(name, data, file_size, eligible, "Replaced")
            if state:
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
            # If None, error already logged, skip file

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