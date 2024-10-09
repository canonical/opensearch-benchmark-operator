#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
from unittest.mock import MagicMock, PropertyMock, patch

import pytest
from ops.testing import Harness

from benchmark.constants import DPBenchmarkExecStatus
from benchmark.service import DPBenchmarkService
from benchmark.status import BenchmarkStatus
from charm import OpenSearchBenchmarkOperator

logger = logging.getLogger(__name__)

OPENSEARCH_APP_NAME = "opensearch"


@pytest.fixture
def harness():
    harness = Harness(OpenSearchBenchmarkOperator)
    with patch("ops.model.Model.name", new_callable=PropertyMock) as mock_name:
        mock_name.return_value = "test_model"
        harness.begin()

    return harness


def test_app_status(harness):
    relation_name = "opensearch"
    svc = MagicMock(DPBenchmarkService)
    charm = harness.charm
    benchmark_status = BenchmarkStatus(charm, relation_name, svc)

    harness.add_relation(relation_name, "opensearch")
    relation = harness.model.get_relation(relation_name)
    relation.data[charm.app]["status"] = DPBenchmarkExecStatus.RUNNING.value

    assert benchmark_status.app_status() == DPBenchmarkExecStatus.RUNNING


def test_unit_status(harness):
    relation_name = "opensearch"
    svc = MagicMock(DPBenchmarkService)
    charm = harness.charm
    benchmark_status = BenchmarkStatus(charm, relation_name, svc)

    harness.add_relation(relation_name, "opensearch")
    relation = harness.model.get_relation(relation_name)
    relation.data[charm.unit]["status"] = DPBenchmarkExecStatus.RUNNING.value

    assert benchmark_status.unit_status() == DPBenchmarkExecStatus.RUNNING


def test_set_status(harness):
    relation_name = "opensearch"
    svc = MagicMock(DPBenchmarkService)
    charm = harness.charm
    benchmark_status = BenchmarkStatus(charm, relation_name, svc)

    harness.add_relation(relation_name, "opensearch")
    relation = harness.model.get_relation(relation_name)
    harness.set_leader(True)

    benchmark_status.set(DPBenchmarkExecStatus.RUNNING)
    assert relation.data[charm.app]["status"] == DPBenchmarkExecStatus.RUNNING.value
    assert relation.data[charm.unit]["status"] == DPBenchmarkExecStatus.RUNNING.value


def test_has_error_happened(harness):
    relation_name = "opensearch"
    svc = MagicMock(DPBenchmarkService)
    charm = harness.charm
    benchmark_status = BenchmarkStatus(charm, relation_name, svc)

    harness.add_relation(relation_name, "opensearch")
    relation = harness.model.get_relation(relation_name)
    relation.data[charm.unit]["status"] = DPBenchmarkExecStatus.ERROR.value

    assert benchmark_status._has_error_happened()


def test_service_status(harness):
    relation_name = "opensearch"
    svc = MagicMock(DPBenchmarkService)
    svc.is_prepared.return_value = True
    svc.is_failed.return_value = False
    svc.is_running.return_value = True
    svc.is_stopped.return_value = False

    charm = harness.charm
    benchmark_status = BenchmarkStatus(charm, relation_name, svc)

    assert benchmark_status.service_status() == DPBenchmarkExecStatus.RUNNING


def test_check_status(harness):
    relation_name = "opensearch"
    svc = MagicMock(DPBenchmarkService)
    charm = harness.charm
    benchmark_status = BenchmarkStatus(charm, relation_name, svc)

    harness.add_relation(relation_name, "opensearch")
    relation = harness.model.get_relation(relation_name)
    harness.set_leader(True)

    svc.is_prepared.return_value = True
    svc.is_failed.return_value = False
    svc.is_running.return_value = True
    svc.is_stopped.return_value = False

    relation.data[charm.app]["status"] = DPBenchmarkExecStatus.RUNNING.value
    relation.data[charm.unit]["status"] = DPBenchmarkExecStatus.RUNNING.value

    assert benchmark_status.check() == DPBenchmarkExecStatus.RUNNING
