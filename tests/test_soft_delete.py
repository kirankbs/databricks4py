"""Tests for the soft delete merge action."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from databricks4py.io.merge import MergeBuilder


def _builder() -> MergeBuilder:
    with patch("databricks4py.io.merge.active_fallback", return_value=MagicMock()):
        return MergeBuilder("catalog.schema.target", MagicMock())


@pytest.mark.no_pyspark
class TestWhenMatchedSoftDeleteChaining:
    def test_returns_builder(self) -> None:
        b = _builder()
        result = b.when_matched_soft_delete()
        assert result is b

    def test_default_columns(self) -> None:
        b = _builder()
        b.when_matched_soft_delete()
        action = b._actions[-1]
        assert action["type"] == "matched_soft_delete"
        assert action["deleted_col"] == "is_deleted"
        assert action["deleted_at_col"] == "deleted_at"
        assert action["condition"] is None

    def test_custom_deleted_col(self) -> None:
        b = _builder()
        b.when_matched_soft_delete(deleted_col="archived")
        assert b._actions[-1]["deleted_col"] == "archived"

    def test_custom_deleted_at_col(self) -> None:
        b = _builder()
        b.when_matched_soft_delete(deleted_at_col="archived_at")
        assert b._actions[-1]["deleted_at_col"] == "archived_at"

    def test_no_timestamp_col(self) -> None:
        b = _builder()
        b.when_matched_soft_delete(deleted_at_col=None)
        assert b._actions[-1]["deleted_at_col"] is None

    def test_empty_deleted_col_raises(self) -> None:
        b = _builder()
        with pytest.raises(ValueError, match="deleted_col"):
            b.when_matched_soft_delete(deleted_col="")

    def test_empty_deleted_at_col_raises(self) -> None:
        b = _builder()
        with pytest.raises(ValueError, match="deleted_at_col"):
            b.when_matched_soft_delete(deleted_at_col="")

    def test_none_deleted_at_col_is_fine(self) -> None:
        b = _builder()
        b.when_matched_soft_delete(deleted_at_col=None)  # must not raise
        assert b._actions[-1]["deleted_at_col"] is None

    def test_with_condition(self) -> None:
        b = _builder()
        b.when_matched_soft_delete(condition="target.version < source.version")
        assert b._actions[-1]["condition"] == "target.version < source.version"

    def test_chainable_with_other_actions(self) -> None:
        b = _builder()
        b.on("id").when_not_matched_insert().when_matched_soft_delete()
        assert len(b._actions) == 2
        assert b._actions[0]["type"] == "not_matched_insert"
        assert b._actions[1]["type"] == "matched_soft_delete"


@pytest.mark.no_pyspark
class TestApplyActionSoftDelete:
    def _apply(self, action: dict, condition: str | None = None) -> MagicMock:
        merger = MagicMock()
        merger.whenMatchedUpdate.return_value = merger
        b = _builder()
        b._apply_action(merger, action)
        return merger

    def test_sets_deleted_col_true(self) -> None:
        merger = MagicMock()
        merger.whenMatchedUpdate.return_value = merger
        b = _builder()
        b._apply_action(
            merger,
            {
                "type": "matched_soft_delete",
                "deleted_col": "is_deleted",
                "deleted_at_col": None,
                "condition": None,
            },
        )
        merger.whenMatchedUpdate.assert_called_once_with(set={"is_deleted": "true"})

    def test_sets_deleted_at_col(self) -> None:
        merger = MagicMock()
        merger.whenMatchedUpdate.return_value = merger
        b = _builder()
        b._apply_action(
            merger,
            {
                "type": "matched_soft_delete",
                "deleted_col": "is_deleted",
                "deleted_at_col": "deleted_at",
                "condition": None,
            },
        )
        called_set = merger.whenMatchedUpdate.call_args[1]["set"]
        assert called_set["is_deleted"] == "true"
        assert called_set["deleted_at"] == "current_timestamp()"

    def test_condition_forwarded(self) -> None:
        merger = MagicMock()
        merger.whenMatchedUpdate.return_value = merger
        b = _builder()
        b._apply_action(
            merger,
            {
                "type": "matched_soft_delete",
                "deleted_col": "deleted",
                "deleted_at_col": None,
                "condition": "target.status = 'active'",
            },
        )
        merger.whenMatchedUpdate.assert_called_once_with(
            condition="target.status = 'active'",
            set={"deleted": "true"},
        )

    def test_no_condition_no_kwarg(self) -> None:
        """When condition is None, `condition` kwarg must not be passed."""
        merger = MagicMock()
        merger.whenMatchedUpdate.return_value = merger
        b = _builder()
        b._apply_action(
            merger,
            {
                "type": "matched_soft_delete",
                "deleted_col": "del",
                "deleted_at_col": None,
                "condition": None,
            },
        )
        # Must not have 'condition' in the call kwargs
        assert "condition" not in merger.whenMatchedUpdate.call_args[1]

    def test_unknown_action_still_raises(self) -> None:
        b = _builder()
        with pytest.raises(ValueError, match="Unknown merge action"):
            b._apply_action(MagicMock(), {"type": "explode"})
