#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

from unittest.mock import MagicMock, PropertyMock, patch

import pytest
from ops.testing import Harness

from benchmark.constants import DPBenchmarkBaseDatabaseModel, DPBenchmarkExecutionModel
from benchmark.service import DPBenchmarkService
from charm import OpenSearchBenchmarkOperator


@pytest.fixture
def harness():
    harness = Harness(OpenSearchBenchmarkOperator)

    with patch("ops.model.Model.name", new_callable=PropertyMock) as mock_name:
        mock_name.return_value = "test_model"
        harness.begin()

    return harness


def test_is_prepared(harness):
    with patch("os.path.exists") as mock_exists:
        service = DPBenchmarkService()
        mock_exists.return_value = True
        assert service.is_prepared()
        mock_exists.return_value = False
        assert not service.is_prepared()


def test_render_service_executable(harness):
    with patch("shutil.copyfile") as mock_copyfile, patch("os.chmod") as mock_chmod:
        service = DPBenchmarkService()
        service.render_service_executable()
        mock_copyfile.assert_called_once_with(
            "templates/dpe_benchmark.py", "/usr/bin/dpe_benchmark.py"
        )
        mock_chmod.assert_called_once_with("/usr/bin/dpe_benchmark.py", 0o755)


def test_render_service_file(harness):
    with (
        patch("benchmark.service._render") as mock_render,
        patch("benchmark.service.daemon_reload") as mock_daemon_reload,
    ):
        service = DPBenchmarkService()
        db = MagicMock(spec=DPBenchmarkExecutionModel)
        db.db_info = MagicMock(spec=DPBenchmarkBaseDatabaseModel)

        db.db_info.hosts = "localhost"
        db.db_info.workload_name = "workload"
        db.threads = 4
        db.clients = 10
        db.db_info.username = "user"
        db.db_info.password = "pass"
        db.duration = 60
        db.db_info.workload_params = "params"
        mock_daemon_reload.return_value = True

        result = service.render_service_file(db)
        assert result
        mock_render.assert_called_once_with(
            "dpe_benchmark.service.j2",
            "/etc/systemd/system/dpe_benchmark.service",
            {
                "target_hosts": "localhost",
                "workload": "workload",
                "threads": 4,
                "clients": 10,
                "db_user": "user",
                "db_password": "pass",
                "duration": 60,
                "workload_params": "params",
                "extra_labels": "",
            },
        )
        mock_daemon_reload.assert_called_once()


def test_is_running(harness):
    with (
        patch("os.path.exists") as mock_exists,
        patch("benchmark.service.service_running") as mock_service_running,
    ):
        service = DPBenchmarkService()
        mock_exists.return_value = True
        mock_service_running.return_value = True
        assert service.is_running()
        mock_service_running.return_value = False
        assert not service.is_running()


def test_is_failed(harness):
    with (
        patch("os.path.exists") as mock_exists,
        patch("benchmark.service.service_failed") as mock_service_failed,
    ):
        service = DPBenchmarkService()
        mock_exists.return_value = True
        mock_service_failed.return_value = True
        assert service.is_failed()
        mock_service_failed.return_value = False
        assert not service.is_failed()


def test_stop(harness):
    with (
        patch("os.path.exists") as mock_exists,
        patch("benchmark.service.service_stop") as mock_service_stop,
    ):
        service = DPBenchmarkService()
        mock_exists.return_value = True
        mock_service_stop.return_value = True
        with patch.object(service, "is_running", return_value=True):
            assert service.stop()
        mock_service_stop.assert_called_once()


def test_run(harness):
    with (
        patch("os.path.exists") as mock_exists,
        patch("benchmark.service.service_restart") as mock_service_restart,
    ):
        service = DPBenchmarkService()
        mock_exists.return_value = True
        mock_service_restart.return_value = True
        with patch.object(service, "is_stopped", return_value=True):
            assert service.run()
        mock_service_restart.assert_called_once()


def test_unset(harness):
    with (
        patch("os.remove") as mock_remove,
        patch("benchmark.service.daemon_reload") as mock_daemon_reload,
        patch("benchmark.service.service_stop") as mock_service_stop,
        patch("benchmark.service.service_running") as mock_service_running,
    ):
        service = DPBenchmarkService()
        service.is_prepared = MagicMock(return_value=True)

        mock_service_running.return_value = True

        mock_service_stop.return_value = True
        mock_daemon_reload.return_value = True
        assert service.unset()
        mock_service_stop.assert_called_once()
        mock_remove.assert_called_once_with(service.svc_path)
        mock_daemon_reload.assert_called_once()
