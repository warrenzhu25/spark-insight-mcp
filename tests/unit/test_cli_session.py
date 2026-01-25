"""
Tests for CLI session management and numbered app references.
"""

import json
import time
from unittest.mock import patch

import pytest

from spark_history_mcp.cli.session import (
    SESSION_TIMEOUT_SECONDS,
    clear_app_refs,
    format_session_hint,
    is_number_ref,
    load_app_refs,
    resolve_number_ref,
    save_app_refs,
)


class TestIsNumberRef:
    """Tests for is_number_ref function."""

    def test_valid_single_digit(self):
        """Single digit positive integers are valid."""
        assert is_number_ref("1") is True
        assert is_number_ref("5") is True
        assert is_number_ref("9") is True

    def test_valid_multi_digit(self):
        """Multi-digit positive integers are valid."""
        assert is_number_ref("10") is True
        assert is_number_ref("99") is True
        assert is_number_ref("123") is True

    def test_zero_is_invalid(self):
        """Zero is not a valid reference."""
        assert is_number_ref("0") is False

    def test_negative_numbers_invalid(self):
        """Negative numbers are not valid."""
        assert is_number_ref("-1") is False
        assert is_number_ref("-10") is False

    def test_leading_zeros_invalid(self):
        """Numbers with leading zeros are not valid."""
        assert is_number_ref("01") is False
        assert is_number_ref("001") is False
        assert is_number_ref("0123") is False

    def test_non_numeric_invalid(self):
        """Non-numeric strings are not valid."""
        assert is_number_ref("a") is False
        assert is_number_ref("abc") is False
        assert is_number_ref("1a") is False
        assert is_number_ref("a1") is False
        assert is_number_ref("1.5") is False

    def test_app_id_patterns_invalid(self):
        """App ID patterns are not valid number refs."""
        assert is_number_ref("app-123") is False
        assert is_number_ref("app-20231201-123456") is False
        assert is_number_ref("application_1234567890_001") is False

    def test_empty_string_invalid(self):
        """Empty string is not valid."""
        assert is_number_ref("") is False

    def test_whitespace_invalid(self):
        """Whitespace is not valid."""
        assert is_number_ref(" ") is False
        assert is_number_ref(" 1") is False
        assert is_number_ref("1 ") is False


class TestSaveLoadAppRefs:
    """Tests for save_app_refs and load_app_refs functions."""

    @pytest.fixture
    def temp_session_dir(self, tmp_path):
        """Create a temporary session directory."""
        with patch(
            "spark_history_mcp.cli.session.get_session_dir", return_value=tmp_path
        ):
            with patch(
                "spark_history_mcp.cli.session.get_app_refs_file",
                return_value=tmp_path / "app-refs-session.json",
            ):
                yield tmp_path

    def test_save_and_load_roundtrip(self, temp_session_dir):
        """Test that saving and loading preserves data."""
        mapping = {1: "app-123", 2: "app-456", 3: "app-789"}
        server = "test-server"

        save_app_refs(mapping, server)
        result = load_app_refs()

        assert result is not None
        loaded_mapping, loaded_server = result
        assert loaded_mapping == mapping
        assert loaded_server == server

    def test_save_and_load_without_server(self, temp_session_dir):
        """Test saving and loading without a server."""
        mapping = {1: "app-123"}

        save_app_refs(mapping, None)
        result = load_app_refs()

        assert result is not None
        loaded_mapping, loaded_server = result
        assert loaded_mapping == mapping
        assert loaded_server is None

    def test_load_nonexistent_file_returns_none(self, temp_session_dir):
        """Loading when no session file exists returns None."""
        result = load_app_refs()
        assert result is None

    def test_load_stale_session_returns_none(self, temp_session_dir):
        """Loading a stale (>1 hour old) session returns None."""
        mapping = {1: "app-123"}
        save_app_refs(mapping)

        # Manually modify the timestamp to be old
        session_file = temp_session_dir / "app-refs-session.json"
        with open(session_file, "r") as f:
            data = json.load(f)
        data["timestamp"] = time.time() - SESSION_TIMEOUT_SECONDS - 1
        with open(session_file, "w") as f:
            json.dump(data, f)

        result = load_app_refs()
        assert result is None

    def test_load_corrupted_json_returns_none(self, temp_session_dir):
        """Loading corrupted JSON returns None."""
        session_file = temp_session_dir / "app-refs-session.json"
        with open(session_file, "w") as f:
            f.write("not valid json{{{")

        result = load_app_refs()
        assert result is None


class TestResolveNumberRef:
    """Tests for resolve_number_ref function."""

    @pytest.fixture
    def temp_session_with_data(self, tmp_path):
        """Create a temporary session with test data."""
        with patch(
            "spark_history_mcp.cli.session.get_session_dir", return_value=tmp_path
        ):
            with patch(
                "spark_history_mcp.cli.session.get_app_refs_file",
                return_value=tmp_path / "app-refs-session.json",
            ):
                mapping = {1: "app-first", 2: "app-second", 3: "app-third"}
                save_app_refs(mapping, "test-server")
                yield tmp_path

    def test_resolve_valid_number(self, temp_session_with_data):
        """Resolving a valid number returns the app ID."""
        assert resolve_number_ref(1) == "app-first"
        assert resolve_number_ref(2) == "app-second"
        assert resolve_number_ref(3) == "app-third"

    def test_resolve_invalid_number(self, temp_session_with_data):
        """Resolving an invalid number returns None."""
        assert resolve_number_ref(4) is None
        assert resolve_number_ref(99) is None

    def test_resolve_without_session(self, tmp_path):
        """Resolving without a session returns None."""
        with patch(
            "spark_history_mcp.cli.session.get_session_dir", return_value=tmp_path
        ):
            with patch(
                "spark_history_mcp.cli.session.get_app_refs_file",
                return_value=tmp_path / "app-refs-session.json",
            ):
                assert resolve_number_ref(1) is None


class TestFormatSessionHint:
    """Tests for format_session_hint function."""

    def test_zero_apps_returns_empty(self):
        """Zero apps returns empty string."""
        assert format_session_hint(0) == ""

    def test_one_app_singular(self):
        """One app uses singular form."""
        hint = format_session_hint(1)
        assert "1" in hint
        assert "apps show 1" in hint

    def test_multiple_apps_range(self):
        """Multiple apps show range."""
        hint = format_session_hint(3)
        assert "1-3" in hint
        assert "compare apps 1 2" in hint

    def test_large_count(self):
        """Large count shows full range."""
        hint = format_session_hint(10)
        assert "1-10" in hint


class TestClearAppRefs:
    """Tests for clear_app_refs function."""

    @pytest.fixture
    def temp_session_dir(self, tmp_path):
        """Create a temporary session directory."""
        with patch(
            "spark_history_mcp.cli.session.get_session_dir", return_value=tmp_path
        ):
            with patch(
                "spark_history_mcp.cli.session.get_app_refs_file",
                return_value=tmp_path / "app-refs-session.json",
            ):
                yield tmp_path

    def test_clear_existing_refs(self, temp_session_dir):
        """Clearing existing refs removes the file."""
        mapping = {1: "app-123"}
        save_app_refs(mapping)

        session_file = temp_session_dir / "app-refs-session.json"
        assert session_file.exists()

        clear_app_refs()
        assert not session_file.exists()

    def test_clear_nonexistent_refs(self, temp_session_dir):
        """Clearing when no file exists does not error."""
        session_file = temp_session_dir / "app-refs-session.json"
        assert not session_file.exists()

        clear_app_refs()  # Should not raise
        assert not session_file.exists()
