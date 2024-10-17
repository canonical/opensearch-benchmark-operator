#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

from unittest.mock import patch

from ops.model import ActiveStatus

from benchmark.literals import (
    DatabaseRelationStatus,
)


def test_on_install(harness):
    with (
        patch("os.remove") as mock_remove,
        patch("os.path.exists") as mock_exists,
        patch("benchmark.base_charm.apt") as mock_apt,
        patch("subprocess.check_output") as mock_check_output,
        patch("benchmark.managers.service.DPBenchmarkService.render_service_executable"),
    ):
        mock_exists.return_value = True
        harness.charm._on_install(None)
        mock_apt.update.assert_called()

        mock_apt.add_package.assert_any_call([
            "python3-pip",
            "python3-prometheus-client",
        ])

        mock_exists.assert_called_once_with("/usr/lib/python3.12/EXTERNALLY-MANAGED")
        mock_remove.assert_called_once()
        mock_check_output.assert_any_call(["apt", "purge", "-y", "python3-jinja2"])
        mock_check_output.assert_any_call(["pip3", "install", "opensearch-benchmark"])
        assert isinstance(harness.charm.unit.status, ActiveStatus)
    assert harness.charm.database.check() == DatabaseRelationStatus.NOT_AVAILABLE


def test_supported_workloads(harness):
    with patch("os.listdir") as mock_listdir:
        mock_listdir.return_value = [
            "workload1.template.json",
            "workload2.template.json",
            "workload3.template.json",
        ]
        expected_workloads = ["workload1", "workload2", "workload3"]
        assert harness.charm.supported_workloads() == expected_workloads
