"""Unit tests for b2backup with mocked B2 backend."""
import pytest
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch, PropertyMock
from dataclasses import dataclass

from b2backup.config import Account, load_config
from b2backup.storage import B2AccountPool, AccountState


class MockFileVersion:
    def __init__(self, file_name, size, upload_timestamp, content_type="application/octet-stream", id_="file_id_123"):
        self.file_name = file_name
        self.size = size
        self.upload_timestamp = upload_timestamp
        self.content_type = content_type
        self.id_ = id_


class MockBucket:
    def __init__(self, name, files=None):
        self.name = name
        self._files = files or []

    def ls(self, latest_only=True):
        return self._files

    def get_file_info_by_name(self, name):
        for f in self._files:
            if f.file_name == name:
                return f
        raise Exception("File not found")

    def upload_bytes(self, data, name):
        pass

    def delete_file_version(self, file_id, name):
        pass

    def download_file_by_id(self, file_id):
        mock_resp = Mock()
        mock_resp.content = b"test content"
        mock_resp.iter_content = lambda chunk_size: [b"test content"]
        return Mock(response=mock_resp)


def create_mock_account(account_id, bucket_name, files=None, capacity_mb=10240):
    """Create a mock Account with its bucket."""
    account = Account(account_id=account_id, account_key="key", capacity_in_mb=capacity_mb)
    bucket = MockBucket(bucket_name, files)
    return account, bucket


def test_load_config(tmp_path):
    config_file = tmp_path / "accounts.yaml"
    config_file.write_text("""
max_files_per_account: 1700
accounts:
  - account_id: "abc123"
    account_key: "key1"
    capacity_in_mb: 10240
  - account_id: "def456"
    account_key: "key2"
""")
    accounts = load_config(str(config_file))
    assert len(accounts) == 2
    assert accounts[0].account_id == "abc123"
    assert accounts[0].capacity_in_mb == 10240
    assert accounts[1].account_id == "def456"
    assert accounts[1].capacity_in_mb is None


def test_pool_initialization_builds_cache():
    """Test that pool builds cache and detects duplicates at startup."""
    account1, bucket1 = create_mock_account(
        "acc1", "bucket1",
        files=[MockFileVersion("file1.txt", 100, 1000)]
    )
    account2, bucket2 = create_mock_account(
        "acc2", "bucket2",
        files=[MockFileVersion("file2.txt", 200, 2000)]
    )

    with patch("b2backup.storage.B2Api") as mock_api_class:
        mock_api = Mock()
        mock_api_class.return_value = mock_api
        mock_api.list_buckets.side_effect = [[bucket1], [bucket2]]
        mock_api.authorize_account.return_value = None

        pool = B2AccountPool([account1, account2])

    assert len(pool._states) == 2
    assert len(pool._cache) == 2
    assert pool._cache["file1.txt"] == pool._states[0]
    assert pool._cache["file2.txt"] == pool._states[1]
    assert pool._states[0].used_bytes == 100
    assert pool._states[0].file_count == 1
    assert pool._states[1].used_bytes == 200
    assert pool._states[1].file_count == 1


def test_pool_detects_duplicate_at_startup():
    """Test that pool raises on duplicate filenames across accounts."""
    account1, bucket1 = create_mock_account(
        "acc1", "bucket1",
        files=[MockFileVersion("same.txt", 100, 1000)]
    )
    account2, bucket2 = create_mock_account(
        "acc2", "bucket2",
        files=[MockFileVersion("same.txt", 200, 2000)]
    )

    with patch("b2backup.storage.B2Api") as mock_api_class:
        mock_api = Mock()
        mock_api_class.return_value = mock_api
        mock_api.list_buckets.side_effect = [[bucket1], [bucket2]]

        with pytest.raises(RuntimeError, match="CONSISTENCY VIOLATION"):
            B2AccountPool([account1, account2])


def test_pool_skips_accounts_with_wrong_bucket_count():
    """Test that accounts with !=1 bucket are skipped with warning."""
    account1, bucket1 = create_mock_account(
        "acc1", "bucket1",
        files=[MockFileVersion("file1.txt", 100, 1000)]
    )
    account2 = Account(account_id="acc2", account_key="key2", capacity_in_mb=10240)
    # No bucket for account2

    with patch("b2backup.storage.B2Api") as mock_api_class:
        mock_api = Mock()
        mock_api_class.return_value = mock_api
        mock_api.list_buckets.side_effect = [[bucket1], []]  # account2 has 0 buckets

        with patch("b2backup.storage.log") as mock_log:
            pool = B2AccountPool([account1, account2])
            mock_log.warning.assert_called()

    assert len(pool._states) == 1
    assert len(pool._cache) == 1


def test_pick_account_best_fit():
    """Test best-fit account selection."""
    account1, bucket1 = create_mock_account("acc1", "b1", capacity_mb=100)  # 100MB cap
    account2, bucket2 = create_mock_account("acc2", "b2", capacity_mb=1000) # 1000MB cap

    with patch("b2backup.storage.B2Api") as mock_api_class:
        mock_api = Mock()
        mock_api_class.return_value = mock_api
        mock_api.list_buckets.side_effect = [[bucket1], [bucket2]]

        pool = B2AccountPool([account1, account2])

    # Manually set usage to test best-fit
    pool._states[0].used_bytes = 90 * 1024 * 1024  # 90MB used, 10MB free
    pool._states[1].used_bytes = 100 * 1024 * 1024  # 100MB used, 900MB free

    # 5MB file should go to account1 (10MB remaining -> 5MB after = best fit)
    state = pool._pick_account(5 * 1024 * 1024)
    assert state == pool._states[0]


def test_pick_account_excludes_source_for_replacement():
    """Test that replacement picks a DIFFERENT account."""
    account1, bucket1 = create_mock_account("acc1", "b1", capacity_mb=1000)
    account2, bucket2 = create_mock_account("acc2", "b2", capacity_mb=1000)
    account3, bucket3 = create_mock_account("acc3", "b3", capacity_mb=1000)

    with patch("b2backup.storage.B2Api") as mock_api_class:
        mock_api = Mock()
        mock_api_class.return_value = mock_api
        mock_api.list_buckets.side_effect = [[bucket1], [bucket2], [bucket3]]

        pool = B2AccountPool([account1, account2, account3])

    # File exists in account1
    pool._cache["test.txt"] = pool._states[0]

    # Replacement should NOT pick account1
    state = pool._pick_account(100, exclude=pool._states[0])
    assert state != pool._states[0]
    assert state in (pool._states[1], pool._states[2])


def test_upload_new_file(tmp_path):
    """Test uploading a new file."""
    account1, bucket1 = create_mock_account("acc1", "b1", capacity_mb=1000)

    with patch("b2backup.storage.B2Api") as mock_api_class:
        mock_api = Mock()
        mock_api_class.return_value = mock_api
        mock_api.list_buckets.return_value = [bucket1]

        pool = B2AccountPool([account1])

    # Create a temp file
    test_file = tmp_path / "test.txt"
    test_file.write_bytes(b"hello world")

    # Mock the bucket's upload_bytes
    bucket1.upload_bytes = Mock()

    pool.upload_file("new.txt", test_file)

    assert "new.txt" in pool._cache
    assert pool._cache["new.txt"] == pool._states[0]
    assert pool._states[0].file_count == 1
    bucket1.upload_bytes.assert_called_once()


def test_upload_replacement_uses_different_account(tmp_path):
    """Test that replacement uploads to different account."""
    account1, bucket1 = create_mock_account("acc1", "b1", capacity_mb=1000)
    account2, bucket2 = create_mock_account("acc2", "b2", capacity_mb=1000)

    with patch("b2backup.storage.B2Api") as mock_api_class:
        mock_api = Mock()
        mock_api_class.return_value = mock_api
        mock_api.list_buckets.side_effect = [[bucket1], [bucket2]]

        pool = B2AccountPool([account1, account2])

    # Pre-populate cache: file exists in account1
    pool._cache["existing.txt"] = pool._states[0]
    pool._states[0].file_count = 1
    pool._states[0].used_bytes = 100

    # Create a temp file
    test_file = tmp_path / "test.txt"
    test_file.write_bytes(b"hello world")

    # Mock bucket methods
    bucket1.get_file_info_by_name = Mock(return_value=MockFileVersion("existing.txt", 100, 1000))
    bucket1.delete_file_version = Mock()
    bucket2.upload_bytes = Mock()

    pool.upload_file("existing.txt", test_file)

    # Should have uploaded to account2
    bucket2.upload_bytes.assert_called_once()
    # Cache should point to account2
    assert pool._cache["existing.txt"] == pool._states[1]
    # Account1 stats should be decremented
    assert pool._states[0].file_count == 0
    assert pool._states[0].used_bytes == 0


def _create_pool_with_cache(account, bucket, files):
    """Helper to create a pool with pre-populated cache, skipping real init."""
    with patch("b2backup.storage.B2Api") as mock_api_class:
        mock_api = Mock()
        mock_api_class.return_value = mock_api
        mock_api.list_buckets.return_value = [bucket]
        mock_api.authorize_account.return_value = None

        pool = B2AccountPool([account])

    # Override cache with our test files
    pool._cache = {}
    for f in files:
        pool._cache[f.file_name] = pool._states[0]
    pool._states[0].file_count = len(files)
    pool._states[0].used_bytes = sum(f.size for f in files)

    return pool


def test_download_file():
    """Test downloading a file."""
    bucket1 = MockBucket("b1", [MockFileVersion("test.txt", 100, 1000)])
    account1 = Account(account_id="acc1", account_key="key", capacity_in_mb=1000)

    pool = _create_pool_with_cache(account1, bucket1, [MockFileVersion("test.txt", 100, 1000)])

    # Replace with mock to verify calls
    bucket1.get_file_info_by_name = Mock(return_value=MockFileVersion("test.txt", 100, 1000))
    bucket1.download_file_by_id = Mock(return_value=Mock(response=Mock(content=b"test", iter_content=lambda cs: [b"test"])))

    dest = Path("/tmp/test_download.txt")
    pool.download_file("test.txt", dest)

    # Verify download was called
    bucket1.get_file_info_by_name.assert_called_with("test.txt")


def test_delete_file():
    """Test deleting a file."""
    account1, bucket1 = create_mock_account(
        "acc1", "b1",
        files=[MockFileVersion("test.txt", 100, 1000)]
    )

    pool = _create_pool_with_cache(account1, bucket1, [MockFileVersion("test.txt", 100, 1000)])

    bucket1.get_file_info_by_name = Mock(return_value=MockFileVersion("test.txt", 100, 1000))
    bucket1.delete_file_version = Mock()

    pool.delete_file("test.txt")

    assert "test.txt" not in pool._cache
    assert pool._states[0].file_count == 0
    assert pool._states[0].used_bytes == 0
    bucket1.delete_file_version.assert_called_once()


def test_list_files():
    """Test listing all files."""
    account1, bucket1 = create_mock_account(
        "acc1", "b1",
        files=[MockFileVersion("a.txt", 100, 1000), MockFileVersion("b.txt", 200, 2000)]
    )

    pool = _create_pool_with_cache(
        account1, bucket1,
        [MockFileVersion("a.txt", 100, 1000), MockFileVersion("b.txt", 200, 2000)]
    )

    files = pool.list_files()
    assert len(files) == 2
    assert files[0]["name"] == "a.txt"
    assert files[1]["name"] == "b.txt"


def test_get_pool_stats():
    """Test pool statistics."""
    account1, bucket1 = create_mock_account("acc1", "bucket1", capacity_mb=100)
    account2, bucket2 = create_mock_account("acc2", "bucket2", capacity_mb=200)

    with patch("b2backup.storage.B2Api") as mock_api_class:
        mock_api = Mock()
        mock_api_class.return_value = mock_api
        mock_api.list_buckets.side_effect = [[bucket1], [bucket2]]

        pool = B2AccountPool([account1, account2])

    pool._states[0].used_bytes = 50 * 1024 * 1024
    pool._states[0].file_count = 10
    pool._states[1].used_bytes = 100 * 1024 * 1024
    pool._states[1].file_count = 20

    stats = pool.get_pool_stats()

    assert stats["accounts"] == 2
    assert stats["total_files"] == 30
    assert stats["total_bytes"] == 150 * 1024 * 1024
    assert len(stats["per_account"]) == 2
    assert stats["per_account"][0]["remaining_mb"] == 50.0  # 100 - 50
    assert stats["per_account"][1]["remaining_mb"] == 100.0  # 200 - 100


def test_remaining_files_respects_b2_limit():
    """Test that file count respects B2's 1700 limit."""
    account1, bucket1 = create_mock_account("acc1", "b1")

    with patch("b2backup.storage.B2Api") as mock_api_class:
        mock_api = Mock()
        mock_api_class.return_value = mock_api
        mock_api.list_buckets.return_value = [bucket1]

        pool = B2AccountPool([account1])

    pool._states[0].file_count = 1699
    assert pool._remaining_files(pool._states[0]) == 1

    pool._states[0].file_count = 1700
    assert pool._remaining_files(pool._states[0]) == 0

    pool._states[0].file_count = 1701
    assert pool._remaining_files(pool._states[0]) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])