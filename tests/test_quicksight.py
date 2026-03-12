"""Tests for the QuickSight setup module.

All AWS calls are mocked — no live account needed.
"""

import sys
import time as _time_mod
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError


def _make_client_error(code, message=""):
    return ClientError(
        {"Error": {"Code": code, "Message": message}},
        "TestOperation",
    )


@pytest.fixture(scope="module")
def qs_mod():
    """Import setup_quicksight with all AWS and config dependencies mocked."""
    mock_cfg = MagicMock()
    mock_cfg.s3_bucket = "test-bucket"
    mock_cfg.quicksight_user = "testuser"
    mock_cfg.aws_region = "us-west-2"
    mock_cfg.athena_database = "crypto_pipeline"

    mock_sts = MagicMock()
    mock_sts.get_caller_identity.return_value = {"Account": "123456789012"}

    mock_qs = MagicMock()

    def _client_factory(service, **kwargs):
        if service == "sts":
            return mock_sts
        if service == "quicksight":
            return mock_qs
        return MagicMock()

    with (
        patch("utils.config.get_config", return_value=mock_cfg),
        patch("boto3.client", side_effect=_client_factory),
        patch("time.sleep"),
    ):
        import importlib
        import Implementation.quicksight.setup_quicksight as mod

        importlib.reload(mod)

    mod._mock_qs = mock_qs
    return mod


class TestResourceIdKey:
    def test_datasource(self, qs_mod):
        assert qs_mod._resource_id_key("datasource") == "DataSourceId"

    def test_dataset(self, qs_mod):
        assert qs_mod._resource_id_key("dataset") == "DataSetId"

    def test_analysis(self, qs_mod):
        assert qs_mod._resource_id_key("analysis") == "AnalysisId"


class TestGrantPermissions:
    def test_grants_datasource_permissions(self, qs_mod):
        qs_mod._mock_qs.reset_mock()
        qs_mod._grant_permissions("datasource", "test-ds-id")
        qs_mod._mock_qs.update_data_source_permissions.assert_called_once()
        kwargs = qs_mod._mock_qs.update_data_source_permissions.call_args[1]
        assert kwargs["DataSourceId"] == "test-ds-id"
        assert kwargs["AwsAccountId"] == "123456789012"
        actions = kwargs["GrantPermissions"][0]["Actions"]
        assert "quicksight:DescribeDataSource" in actions

    def test_retries_on_creation_in_progress(self, qs_mod):
        qs_mod._mock_qs.reset_mock()
        error = _make_client_error("ConflictException", "CREATION_IN_PROGRESS")
        qs_mod._mock_qs.update_data_set_permissions.side_effect = [
            error,
            error,
            None,
        ]
        qs_mod._grant_permissions("dataset", "test-ds-id")
        assert qs_mod._mock_qs.update_data_set_permissions.call_count == 3
        qs_mod._mock_qs.update_data_set_permissions.side_effect = None

    def test_raises_non_retryable_error(self, qs_mod):
        qs_mod._mock_qs.reset_mock()
        error = _make_client_error("AccessDeniedException", "Not authorized")
        qs_mod._mock_qs.update_data_set_permissions.side_effect = error
        with pytest.raises(ClientError):
            qs_mod._grant_permissions("dataset", "test-ds-id")
        qs_mod._mock_qs.update_data_set_permissions.side_effect = None


class TestCreateDataSource:
    def test_creates_athena_data_source(self, qs_mod):
        qs_mod._mock_qs.reset_mock()
        qs_mod.create_data_source()
        qs_mod._mock_qs.create_data_source.assert_called_once()
        kwargs = qs_mod._mock_qs.create_data_source.call_args[1]
        assert kwargs["Type"] == "ATHENA"
        assert kwargs["DataSourceId"] == "crypto-pipeline-athena"
        qs_mod._mock_qs.update_data_source_permissions.assert_called_once()

    def test_idempotent_on_exists(self, qs_mod):
        qs_mod._mock_qs.reset_mock()
        qs_mod._mock_qs.create_data_source.side_effect = _make_client_error("ResourceExistsException")
        qs_mod.create_data_source()
        qs_mod._mock_qs.update_data_source_permissions.assert_not_called()
        qs_mod._mock_qs.create_data_source.side_effect = None


class TestCreateDatasets:
    def test_creates_two_datasets(self, qs_mod):
        qs_mod._mock_qs.reset_mock()
        qs_mod.create_datasets()
        assert qs_mod._mock_qs.create_data_set.call_count == 2
        ids = [c[1]["DataSetId"] for c in qs_mod._mock_qs.create_data_set.call_args_list]
        assert "crypto-raw-trades" in ids
        assert "crypto-candles-1m" in ids

    def test_dataset_uses_spice(self, qs_mod):
        qs_mod._mock_qs.reset_mock()
        qs_mod.create_datasets()
        for c in qs_mod._mock_qs.create_data_set.call_args_list:
            assert c[1]["ImportMode"] == "SPICE"

    def test_idempotent_on_exists(self, qs_mod):
        qs_mod._mock_qs.reset_mock()
        qs_mod._mock_qs.create_data_set.side_effect = _make_client_error("ResourceExistsException")
        qs_mod.create_datasets()
        qs_mod._mock_qs.update_data_set_permissions.assert_not_called()
        qs_mod._mock_qs.create_data_set.side_effect = None


class TestCreateAnalysis:
    def test_creates_analysis_with_visuals(self, qs_mod):
        qs_mod._mock_qs.reset_mock()
        qs_mod.create_analysis()
        qs_mod._mock_qs.create_analysis.assert_called_once()
        kwargs = qs_mod._mock_qs.create_analysis.call_args[1]
        assert kwargs["AnalysisId"] == "crypto-pipeline-analysis"
        assert kwargs["Name"] == "Crypto Pipeline Dashboard"
        definition = kwargs["Definition"]
        assert len(definition["DataSetIdentifierDeclarations"]) == 2
        sheets = definition["Sheets"]
        assert len(sheets) == 1
        assert len(sheets[0]["Visuals"]) == 3

    def test_idempotent_on_exists(self, qs_mod):
        qs_mod._mock_qs.reset_mock()
        qs_mod._mock_qs.create_analysis.side_effect = _make_client_error("ResourceExistsException")
        qs_mod.create_analysis()
        qs_mod._mock_qs.update_analysis_permissions.assert_not_called()
        qs_mod._mock_qs.create_analysis.side_effect = None


class TestTriggerSpiceRefresh:
    def test_triggers_two_ingestions(self, qs_mod):
        qs_mod._mock_qs.reset_mock()
        qs_mod.trigger_spice_refresh()
        assert qs_mod._mock_qs.create_ingestion.call_count == 2
        ds_ids = [c[1]["DataSetId"] for c in qs_mod._mock_qs.create_ingestion.call_args_list]
        assert "crypto-raw-trades" in ds_ids
        assert "crypto-candles-1m" in ds_ids

    def test_handles_already_running(self, qs_mod):
        qs_mod._mock_qs.reset_mock()
        qs_mod._mock_qs.create_ingestion.side_effect = _make_client_error("ResourceExistsException")
        qs_mod.trigger_spice_refresh()
        assert qs_mod._mock_qs.create_ingestion.call_count == 2
        qs_mod._mock_qs.create_ingestion.side_effect = None


class TestPrintUrls:
    def test_prints_analysis_url(self, qs_mod, capsys):
        qs_mod.print_urls()
        output = capsys.readouterr().out
        assert "crypto-pipeline-analysis" in output
        assert "quicksight.aws.amazon.com" in output
