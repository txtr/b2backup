# b2backup

A minimal, single-process CLI tool for backing up directories to a pool of Backblaze B2 accounts. Designed for personal use on trusted networks.

## Design Philosophy

| Principle | Decision |
|-----------|----------|
| **Simplicity over features** | ~600 LOC total; no daemon, no HTTP, no job queue |
| **Trust the user** | No auth, no rate limits, no CSRF, no CSP |
| **B2 as source of truth** | Startup scan validates consistency; cache updated on mutations |
| **Atomic upserts** | Replacements always go to a *different* account; old version deleted after |
| **Best-fit placement** | Pack files into accounts with least remaining space (bin-packing) |
| **Strict validation** | Hard stop on any inconsistency (bucket count, versions, duplicates) |
| **Observable** | Structured logging; `stats` command shows capacity per account |

---

## Constraints & Assumptions

### Backblaze B2 Constraints
- **File-version budget per account** (configurable via `max_files_per_account`, default 1,700; B2's list API paginates up to 10,000 entries per request)
- **One bucket per account**: Startup requires each account to expose exactly one bucket
- **Master application keys**: Keys are assumed unrestricted; isolation comes from using one bucket per account
- **Multiple accounts**: Workaround for a single unified namespace = multiple accounts, each with one bucket
- **All versions count against limits**: Hidden versions consume file slots and bytes
- **Eventual consistency**: `list_buckets` / `ls` may lag seconds after mutations

### Operational Constraints
- **Single writer**: No locking; assume one `b2backup` process at a time
- **No external mutations**: Cache assumes no other writers (other tools, manual B2 UI)
- **Trusted network**: No TLS, no auth — deploy behind VPN/SSH tunnel if remote
- **Memory-bound cache**: Entire file index in RAM (≈ 200 bytes/file → 2M files ≈ 400MB)

### Not Supported (by design)
- Resumable uploads (re-upload on failure)
- Parallel uploads (single-threaded, sequential)
- Encryption at rest (use B2 SSE or encrypt client-side before backup)
- Deduplication (content-addressable; we use path-based names)
- Snapshots / versioning (B2 keeps hidden versions; we only track latest)

---

## Project Structure

```
b2backup/
├── pyproject.toml          # Package metadata, deps, entry point
├── README.md               # This file
├── LICENSE                 # MIT license
├── accounts.yaml.example   # Template config
└── b2backup/
    ├── __init__.py         # Version
    ├── config.py           # YAML loading → Account dataclasses
    ├── storage.py          # B2AccountPool: cache, routing, mutations
    └── cli.py              # Click commands: backup, restore, ls, stats
```

### Module Responsibilities

| Module | Responsibility | Key Classes/Functions |
|--------|----------------|----------------------|
| `config.py` | Parse `accounts.yaml` | `Account`, `load_config()` |
| `storage.py` | All B2 logic, cache, routing | `B2AccountPool`, `AccountState`, `BucketValidationError`, `ConsistencyError` |
| `cli.py` | CLI UX, progress bars, output | `backup`, `restore`, `ls`, `stats` |

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        b2backup CLI                              │
├─────────────────────────────────────────────────────────────────┤
│  Click command parser                                            │
│  ├─ backup   → walk dir → upload_file() for each                │
│  ├─ restore  → download_file()                                  │
│  ├─ ls       → list_files() (names from cache, metadata live)   │
│  └─ stats    → get_pool_stats() (from cache)                    │
└────────────────────────────────┬────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                      B2AccountPool                               │
├─────────────────────────────────────────────────────────────────┤
│  Startup (strict validation - HARD STOP on any inconsistency):  │
│  1. Authorize each account_id/account_key                       │
│  2. list_buckets() → MUST return exactly 1 bucket per account   │
│     → If ANY account has ≠1 bucket: print offenders → exit 1    │
│  3. For each bucket: ls(latest_only=False, recursive=True)      │
│     - Sum used_bytes, file_count (ALL versions count)           │
│     - If ANY file has >1 version in SAME account:               │
│       print per-account breakdown → exit 2                      │
│     - Pick latest version per filename for cross-account check  │
│  4. Cross-account duplicate check on latest versions            │
│     → If SAME filename (latest) in ≥2 accounts:                 │
│       print conflicts with timestamps → exit 2                  │
│  5. If ALL checks pass: log pool summary → proceed              │
│                                                                  │
│  Runtime:                                                        │
│  - _get_eligible_accounts_sorted(size, exclude) → best-fit list │
│  - upload_file() → try accounts in order (failover on error)    │
│  - Cache updated on every mutation                              │
└────────────────────────────────┬────────────────────────────────┘
                                 │
                    ┌────────────┼────────────┐
                    ▼            ▼            ▼
              ┌─────────┐  ┌─────────┐  ┌─────────┐
              │Account 1│  │Account 2│  │Account N│
              │(bucket) │  │(bucket) │  │(bucket) │
              └─────────┘  └─────────┘  └─────────┘
```

---

## Data Model

### `Account` (from config)
```python
@dataclass
class Account:
    account_id: str          # B2 keyID (e.g., "ce6c123a9349")
    account_key: str         # B2 applicationKey
    capacity_in_mb: int | None  # Soft limit; None = unlimited
```

### `AccountState` (runtime, per account)
```python
@dataclass
class AccountState:
    account: Account
    bucket_name: str
    bucket: Bucket           # b2sdk Bucket handle
    used_bytes: int = 0      # Sum of ALL version sizes (for capacity)
    file_count: int = 0      # Count of ALL versions (for 1700-version budget)
```

### Cache
```python
# In B2AccountPool:
_cache: dict[str, AccountState]   # filename (latest) → owning AccountState
_states: list[AccountState]       # All valid accounts
```

**Invariant**: `name in _cache` ⟺ file's LATEST version exists in exactly one account.

---

## Command Logic Flows

### 1. `b2backup backup /path/to/dir`

```
1. Resolve source path
2. root_name = source.name (e.g., /ABC/CDE/EFG -> "EFG")
3. Walk source.rglob("*")
4. Filter is_file()
5. If no files: print "No files"; exit 0
6. For each file (with tqdm progress bar):
     rel = file.relative_to(source.parent)  # EFG/HIJ/KLM/file.ext
     pool.upload_file(str(rel), file)
7. Print "Backup complete"
```

**`pool.upload_file(name, local_path)`**:
```
upload_file(name, path)
    │
    ├─ file_size = path.stat().st_size
    ├─ existing = _cache.get(name)
    │
    ├─ if existing is None:                    # NEW FILE
    │     eligible = _get_eligible_accounts_sorted(file_size)
    │     if none: log error; skip file
    │     for state in eligible:               # failover
    │         try: state.bucket.upload_bytes(data, name)
    │         on success:
    │             state.used_bytes += file_size
    │             state.file_count += 1
    │             _cache[name] = state
    │             return
    │
    └─ else:                                   # REPLACEMENT
          eligible = _get_eligible_accounts_sorted(file_size, exclude=existing)
          if none: log error; skip file
          for state in eligible:               # failover, never the owner
              try: state.bucket.upload_bytes(data, name)
              on success:
                  state.used_bytes += file_size
                  state.file_count += 1
                  _cache[name] = state
                  try:
                      old = existing.bucket.get_file_info_by_name(name)
                      existing.bucket.delete_file_version(old.id_, name)
                      existing.used_bytes -= old.size
                      existing.file_count -= 1
                  except: log warning (duplicate remains)
```

**`_get_eligible_accounts_sorted(size, exclude)`**:
```
_eligible_accounts(size, exclude)
    For each state in _states (skip exclude):
        rem_bytes = capacity - used_bytes (if capacity set)
        rem_files = 1700 - file_count
        if rem_bytes >= size and rem_files > 0:
            add to eligible
    return eligible

Sort by remaining_after = rem_bytes - size (unlimited last)
Tie-break: account_id (deterministic)
upload_file tries accounts in that order until an upload succeeds.
```

---

### 2. `b2backup restore "EFG/HIJ/file.ext" ./dest/`

```
1. Resolve destination; if it is an existing directory or ends with "/",
   append the source filename (e.g. ./dest/ -> ./dest/file.ext)
2. pool.download_file(name, dest_path)
3. _cache.get(name)
4. If not found: raise FileNotFoundError
5. state.bucket.get_file_info_by_name(name)
6. state.bucket.download_file_by_id(file_id)
7. dest_path.write_bytes(content)
8. Done
```

---

### 3. `b2backup ls`

```
1. pool.list_files()
2. For each name, state in sorted(_cache):
     file_obj = state.bucket.get_file_info_by_name(name)
     Collect: name, size, timestamp, content_type, account_id
3. Return list of dicts
4. CLI prints formatted table
```

> **Note**: `list_files()` does a live `get_file_info_by_name` per file to get fresh metadata. For large pools, consider caching metadata at startup (trade memory for speed).

---

### 4. `b2backup stats`

```
1. pool.get_pool_stats()
2. Aggregate totals from _states
3. For each state: compute remaining_bytes, remaining_files
4. Return dict with totals + per-account breakdown
5. CLI prints formatted table
```

---

## Startup Sequence (Critical Path)

```
1. User runs: b2backup --config=accounts.yaml backup /data
2. CLI creates B2AccountPool(accounts)
3. Pool authorizes each account (× N)
4. Pool calls list_buckets() for each account (× N)
5. CHECK 1: Each account MUST have exactly 1 bucket
   - If FAIL: Print offender table → exit 1
6. Pool calls bucket.ls(latest_only=False, recursive=True) for each bucket (× N)
7. Aggregate used_bytes, file_count (ALL versions)
8. CHECK 2: No file has >1 version in same account
   - If FAIL: Print per-account breakdown → exit 2
9. Pick latest per filename for cross-account check
10. CHECK 3: No filename (latest) in ≥2 accounts
    - If FAIL: Print conflicts with timestamps → exit 2
11. If ALL CHECKS PASS:
     - Log pool summary
     - Run backup() → upload_file() × M files
```

---

## Routing Algorithm (Best-Fit)

```
Goal: Pack files tightly, leave large contiguous gaps for future large files.

For each account with capacity_in_mb set:
    remaining_bytes = capacity_mb * 1024^2 - used_bytes (ALL versions)
For unlimited accounts:
    remaining_bytes = ∞ (sorted last)

remaining_files = 1700 - file_count (ALL versions count)

Eligible iff:
    remaining_bytes >= file_size (or unlimited)
    remaining_files > 0
    account != exclude (for replacements)

Pick account minimizing: remaining_after = remaining_bytes - file_size
Tie-break: account_id (lexicographic, deterministic)
```

**Why best-fit?** Analogous to bin-packing. Worst-fit leaves fragmented small gaps; first-fit creates hot spots. Best-fit maximizes chance large future files fit.

---

## Consistency Guarantees

| Scenario | Guarantee |
|----------|-----------|
| **Startup: bucket count** | Hard stop if any account has ≠1 bucket; prints offender table |
| **Startup: intra-account versions** | Hard stop if any file has >1 version in same account; prints breakdown |
| **Startup: cross-account duplicates** | Hard stop if same filename (latest) in ≥2 accounts; prints conflict |
| **New file** | Written to exactly one account; cache updated atomically in-process |
| **Replace file** | New version written to *different* account; old deleted after; cache points to new |
| **Crash mid-upload** | Partial upload leaves no cache entry; B2 may have orphan object |
| **Crash mid-replace** | New version in account B, old in account A → duplicate at next startup (caught) |

**Recovery**: If startup fails, manually resolve in B2 console:
- Bucket count: Remove or transfer extra buckets so the account has exactly one
- Multiple versions: Delete old versions via B2 console/CLI
- Cross-account duplicates: Delete from all but one account
- Hide markers: Permanently delete old versions (including hide markers) via B2 console or `b2 delete-file-version`

---

## Configuration

### `accounts.yaml`
```yaml
max_files_per_account: 1700    # Ignored (legacy field)

accounts:
  - account_id: "ce6c123a9349"
    account_key: "005df4727d45a2ff206e29a7882b5e123e6f9954a7"
    capacity_in_mb: 10240      # Optional soft limit
  - account_id: "c5af2a9f177f"
    account_key: "00514c7bfc281fdfd9a75b2022ab08f043abeccfea"
    # capacity_in_mb omitted = unlimited
```

Each configured account must contain **exactly one bucket**. Keys are expected
to be unrestricted master application keys; isolation comes from keeping one
bucket per account. Config values are validated strictly:
`capacity_in_mb` must be a non-negative integer, credentials must be non-empty
strings, and duplicate `account_id` values are rejected.

### Field Reference

| Field | Required | Description |
|-------|----------|-------------|
| `account_id` | Yes | B2 account ID (12-char hex) |
| `account_key` | Yes | B2 master application key |
| `capacity_in_mb` | No | Non-negative soft budget; omitted = unlimited |
| `max_files_per_account` | No | Global file-version budget per account (default: 1700) |

---

## Installation & Usage

```bash
# Clone and install
git clone <this-repo> b2backup
cd b2backup
pip install -e .

# Copy config template
cp accounts.yaml.example accounts.yaml
# Edit with your B2 keys (chmod 600)

# Backup a directory
b2backup --config=accounts.yaml backup /home/user/photos

# List all backed up files
b2backup --config=accounts.yaml ls

# Show pool capacity
b2backup --config=accounts.yaml stats

# Restore a file
b2backup --config=accounts.yaml restore "photos/2024/IMG_123.jpg" ./restored/
```

### Filename Convention

```
Source:     /home/user/photos/2024/IMG_123.jpg
Backup as:  photos/2024/IMG_123.jpg
            ^^^^^^
            root = last component of source path
```

---

## Logging

Uses Python `logging` with `INFO` default. Key events:

```
INFO: Pool ready: 47 accounts, 12847 unique files
INFO:   ce6c123a (backup-1): 283 files, 1024MB used, remaining: 9216MB, 1417 slots
INFO: Created photos/2024/IMG_123.jpg in backup-3 (2.4MB)
INFO: Replaced docs/report.pdf: backup-1 -> backup-7
WARNING: Replaced notes.txt but failed to delete old version: ...
```

Set `B2BACKUP_LOG_LEVEL=DEBUG` for b2sdk HTTP traces. The level is read when
`b2backup.storage` is imported.

---

## Testing

```bash
# Unit tests (mocked B2 backend; no real credentials required)
pytest b2backup/tests/
```

There is no live integration test suite in this repository. Tests use a fake
B2 backend, so they do not exercise real network/API behavior.

Test coverage targets:
- Routing logic (best-fit, exclusion, capacity bounds)
- Cache consistency (duplicates, updates on mutate)
- CLI argument parsing and path handling
- Error paths (missing file, no space, no eligible account)
- Startup validation (bucket count, intra-versions, cross-duplicates)
- Recursive all-version scanning and b2sdk `ls` tuple handling

---

## Operational Runbook

### Adding a New Account
1. Create a B2 account that contains exactly one bucket
2. Add its master credentials to `accounts.yaml`
3. Run `b2backup --config=accounts.yaml stats` → verify appears in pool

### Removing an Account
1. Run `b2backup --config=accounts.yaml stats` → note file count
2. If files exist: migrate by re-backing up those paths (they'll land elsewhere)
3. Remove from `accounts.yaml`
4. Run `stats` → verify removed

### Capacity Alerting
Monitor `stats` output:
- `remaining_files < 100` → add account or increase `capacity_in_mb`
- `remaining_mb < 1024` → add account or increase limit

### Disaster Recovery
| Failure | Recovery |
|---------|----------|
| Config lost | Recreate from B2: `list_buckets` → get keys → rebuild YAML |
| Cache corrupted | Delete nothing; restart rebuilds from B2 |
| Bucket count violation | Remove or transfer extra buckets so the account has exactly one; restart |
| Multiple versions | Delete old versions in B2 console/CLI; restart |
| Cross-account duplicates | Delete from all but one account; restart |
| Account key revoked | Remove from config; re-backup affected files |

---

## Future Work (Non-Goals for v0.1)

- [ ] Parallel uploads (worker pool)
- [ ] Resumable multipart for >100MB files
- [ ] Client-side encryption (age/rage)
- [ ] Retention policies / snapshot management
- [ ] Config file discovery (`~/.config/b2backup/accounts.yaml`)
- [ ] Shell completions (`b2backup --install-completion`)

---

## License

MIT — see `LICENSE` file.