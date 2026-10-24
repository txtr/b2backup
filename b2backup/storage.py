"""Core B2 storage logic: account pool, cache, routing, and mutations."""
import logging
import os
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from b2sdk.v2 import B2Api, InMemoryAccountInfo, Bucket
from .config import Account, Config

UNLIMITED_REMAINING = 10**18

_LOG_LEVEL = os.environ.get("B2BACKUP_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, _LOG_LEVEL, logging.INFO),
    format="%(levelname)s: %(message)s",
)
log = logging.getLogger(__name__)


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
        lines.append("Hint: Each account must contain exactly one bucket; remove or transfer extra buckets.")
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


class HideMarkerError(Exception):
    """Raised when files with hide markers (deleted files) are detected."""
    def __init__(self, issues: list[tuple[str, str, list[str]]]):
        self.issues = issues  # (account_id, bucket_name, files_with_hide_markers)
        super().__init__(self._format_message())

    def _format_message(self) -> str:
        lines = ["ERROR: Files with hide markers detected (deleted files still have versions)"]
        for acc_id, bucket_name, files in self.issues:
            lines.append(f"  {acc_id} ({bucket_name}): {len(files)} files with hide markers")
            for f in files[:10]:  # Show first 10
                lines.append(f"    {f}")
            if len(files) > 10:
                lines.append(f"    ... and {len(files) - 10} more")
        lines.append("Hint: These files were deleted but hidden versions remain.")
        lines.append("      Use B2 console or CLI to permanently delete old versions.")
        lines.append("      Or run 'b2 delete-file-version' for each hide marker.")
        return "\n".join(lines)


class ParallelOperationError(Exception):
    """Raised when one or more parallel operations fail."""
    def __init__(self, message: str, errors: list[str]):
        self.message = message
        self.errors = errors
        super().__init__(self._format_message())

    def _format_message(self) -> str:
        lines = [f"ERROR: {self.message}"]
        lines.extend(f"  {e}" for e in self.errors)
        return "\n".join(lines)


def _iter_file_versions(bucket: Bucket):
    """Yield every file version in a bucket, recursively.

    b2sdk 2.x ``Bucket.ls`` yields ``(FileVersion, folder_name)`` tuples;
    older releases yielded ``FileVersion`` objects directly. Folder markers
    are synthesized by ``ls`` and are not real files, so skip them.
    """
    for entry in bucket.ls(latest_only=False, recursive=True):
        if isinstance(entry, tuple):
            file_version, folder_name = entry
            if folder_name is not None:
                continue
        else:
            file_version = entry
        yield file_version


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
        3. Check for hide markers (deleted files with remaining versions)
        4. Scan all buckets (latest_only=False) to:
           - Compute true used_bytes/file_count (all versions count)
           - Detect multiple versions of same file within account
           - Build latest-version cache for cross-account duplicate detection
        5. Validate no duplicate filenames across accounts (latest only)
        6. Log pool summary

    Invariants (enforced at startup, maintained on mutations):
        - _cache[name] = owning AccountState for every file's LATEST version
        - No filename exists in more than one account's latest versions
        - used_bytes/file_count reflect ALL versions (for capacity tracking)
    """

    def __init__(self, accounts: list[Account], max_files_per_account: int = 1700, max_workers: int = 10):
        self._states: list[AccountState] = []
        self._cache: dict[str, AccountState] = {}  # filename -> owning AccountState (latest)
        self._max_files_per_account = max_files_per_account

        log.info("Authorizing %d accounts...", len(accounts))

        # Phase 1: Parallel authorize and discover buckets
        account_buckets = self._parallel_authorize(accounts, max_workers)
        log.info("Discovered buckets for %d accounts", len(account_buckets))

        # Phase 2: Validate bucket counts (must be exactly 1)
        bucket_issues = []
        for acc, buckets in account_buckets:
            if len(buckets) != 1:
                bucket_issues.append((acc, len(buckets), [b.name for b in buckets]))

        if bucket_issues:
            raise BucketValidationError(bucket_issues)

        # Phase 2.5: Parallel hide marker check
        log.info("Checking for hide markers...")
        hide_marker_issues = self._parallel_hide_check(account_buckets, max_workers)

        if hide_marker_issues:
            raise HideMarkerError(hide_marker_issues)

        # All accounts valid - create states
        for acc, buckets in account_buckets:
            bucket = buckets[0]
            state = AccountState(
                account=acc,
                bucket_name=bucket.name,
                bucket=bucket,
            )
            self._states.append(state)

        # Phase 3: Parallel scan + build cache + validate consistency + compute usage
        log.info("Scanning %d buckets...", len(self._states))
        self._parallel_scan_and_validate(max_workers)

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

    # --- Parallel startup helpers ---

    def _authorize_account(self, acc: Account) -> tuple[Account, list[Bucket]]:
        """Authorize single account and list its buckets."""
        api = B2Api(InMemoryAccountInfo())
        api.authorize_account("production", acc.account_id, acc.account_key)
        buckets = api.list_buckets()
        return (acc, buckets)

    def _parallel_authorize(self, accounts: list[Account], max_workers: int) -> list[tuple[Account, list[Bucket]]]:
        """Authorize all accounts in parallel, collecting all errors."""
        results = []
        errors = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_acc = {executor.submit(self._authorize_account, acc): acc for acc in accounts}
            for future in as_completed(future_to_acc):
                try:
                    results.append(future.result())
                except Exception as e:
                    acc = future_to_acc[future]
                    errors.append(f"Account {acc.account_id}: {type(e).__name__}: {e}")
        
        if errors:
            raise ParallelOperationError(
                "Authorization failed for some accounts",
                errors
            )
        return results

    def _check_hide_markers(self, acc: Account, bucket: Bucket) -> Optional[tuple[str, str, list[str]]]:
        """Check single bucket for hide markers."""
        files_with_hide = set()
        for f in bucket.ls(latest_only=False, recursive=True):
            if isinstance(f, tuple):
                fv = f[0]
            else:
                fv = f
            if getattr(fv, 'action', 'upload') == 'hide':
                files_with_hide.add(fv.file_name)
        if files_with_hide:
            return (acc.account_id, bucket.name, sorted(files_with_hide))
        return None

    def _parallel_hide_check(self, account_buckets: list[tuple[Account, list[Bucket]]], max_workers: int) -> list:
        """Check all buckets for hide markers in parallel, collecting all errors."""
        issues = []
        errors = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(self._check_hide_markers, acc, buckets[0]): acc for acc, buckets in account_buckets}
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result:
                        issues.append(result)
                except Exception as e:
                    acc = futures[future]
                    errors.append(f"Account {acc.account_id}: {type(e).__name__}: {e}")
        
        if errors:
            raise ParallelOperationError(
                "Hide marker check failed for some accounts",
                errors
            )
        return issues

    def _scan_bucket(self, state: AccountState) -> dict:
        """Scan single bucket for versions, compute usage, return data for cache merge."""
        versions_per_file: dict[str, list] = defaultdict(list)
        scanned_count = 0
        scan_start = time.monotonic()
        
        for f in _iter_file_versions(state.bucket):
            state.used_bytes += f.size
            state.file_count += 1
            versions_per_file[f.file_name].append(f)
            scanned_count += 1
            if scanned_count % 1000 == 0:
                elapsed = time.monotonic() - scan_start
                log.info("  Scanning bucket '%s' (account %s)... %d files scanned (%.1fs)",
                         state.bucket_name, state.account.account_id[:8], scanned_count, elapsed)

        elapsed = time.monotonic() - scan_start
        log.info("  Done: %d files (%.1fMB) in '%s' (%.1fs)",
                 scanned_count, state.used_bytes / 1024 / 1024, state.bucket_name, elapsed)

        return {
            "state": state,
            "versions_per_file": versions_per_file,
            "scanned_count": scanned_count,
        }

    def _parallel_scan_and_validate(self, max_workers: int) -> None:
        """Scan all buckets in parallel, then merge results and validate consistency."""
        # name -> [(owning state, latest FileVersion), ...] for files seen in >1 account
        cross_duplicates: dict[str, list[tuple[AccountState, object]]] = defaultdict(list)
        latest_versions: dict[str, tuple[AccountState, object]] = {}
        intra_duplicates: dict[str, dict[str, list]] = defaultdict(dict)

        # Parallel scan
        scan_results = []
        scan_errors = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(self._scan_bucket, state): state for state in self._states}
            for future in as_completed(futures):
                try:
                    scan_results.append(future.result())
                except Exception as e:
                    state = futures[future]
                    scan_errors.append(f"Account {state.account.account_id}: {type(e).__name__}: {e}")
        
        if scan_errors:
            raise ParallelOperationError(
                "Scan failed for some accounts",
                scan_errors
            )

        # Merge results and validate
        for result in scan_results:
            state = result["state"]
            versions_per_file = result["versions_per_file"]

            for name, versions in versions_per_file.items():
                non_hide_versions = [v for v in versions if getattr(v, 'action', 'upload') != 'hide']

                if len(non_hide_versions) > 1:
                    if state.account.account_id not in intra_duplicates:
                        intra_duplicates[state.account.account_id] = {}
                    intra_duplicates[state.account.account_id][name] = non_hide_versions

                if non_hide_versions:
                    latest = max(non_hide_versions, key=lambda v: v.upload_timestamp)
                    if name in latest_versions:
                        if name not in cross_duplicates:
                            cross_duplicates[name].append(latest_versions[name])
                        cross_duplicates[name].append((state, latest))
                    else:
                        latest_versions[name] = (state, latest)
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
            for name, entries in cross_duplicates.items():
                details.append(f"{name} exists in:")
                for state, latest in entries:
                    details.append(
                        f"  - {state.account.account_id[:8]} ({state.bucket_name}) "
                        f"[latest: {latest.upload_timestamp}]"
                    )
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
        """File slots remaining (configurable per-account budget)."""
        return max(0, self._max_files_per_account - state.file_count)

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
            return (rem - file_size) if rem is not None else UNLIMITED_REMAINING

        return sorted(eligible, key=lambda s: (remaining_after(s), s.account.account_id))

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

    def list_files(self) -> list[dict]:
        """List all files with metadata (live B2 lookup for freshness)."""
        result = []
        for name, state in sorted(self._cache.items()):
            try:
                file_obj = state.bucket.get_file_info_by_name(name)
            except Exception as e:
                log.warning("File '%s' in cache but not in B2 (%s): %s", name, state.bucket_name, e)
                continue
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
