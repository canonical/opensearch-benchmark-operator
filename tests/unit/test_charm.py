#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

from unittest.mock import PropertyMock, patch

import pytest
from ops.model import ActiveStatus
from ops.testing import Harness

from benchmark.benchmark_charm import (
    DatabaseRelationStatus,
)
from charm import OpenSearchBenchmarkOperator


@pytest.fixture
def harness():
    harness = Harness(OpenSearchBenchmarkOperator)

    with patch("ops.model.Model.name", new_callable=PropertyMock) as mock_name:
        mock_name.return_value = "test_model"
        harness.begin()

    return harness


def test_on_install(harness):
    with (
        patch("os.remove") as mock_remove,
        patch("benchmark.benchmark_charm.apt") as mock_apt,
        patch("subprocess.check_output") as mock_check_output,
        patch("benchmark.service.DPBenchmarkService.render_service_executable"),
    ):
        harness.charm._on_install(None)
        mock_apt.update.assert_called()
        mock_apt.add_package.assert_any_call([
            "python3-prometheus-client",
            "python3-jinja2",
            "unzip",
        ])
        mock_apt.add_package.assert_any_call(["python3-pip"])

        mock_remove.assert_called_once()
        mock_check_output.assert_called_once()
        assert isinstance(harness.charm.unit.status, ActiveStatus)
    assert harness.charm.database.check() == DatabaseRelationStatus.NOT_AVAILABLE
