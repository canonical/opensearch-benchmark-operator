#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

from unittest.mock import MagicMock, patch

from benchmark.constants import DPBenchmarkBaseDatabaseModel, DPBenchmarkExecutionModel
from benchmark.service import DPBenchmarkService
from models import OpenSearchExecutionExtraConfigsModel


def test_is_prepared(harness, mock_makedirs):
    with patch("os.path.exists") as mock_exists:
        service = DPBenchmarkService()
        mock_exists.return_value = True
        assert service.is_prepared()
        mock_exists.return_value = False
        assert not service.is_prepared()


def test_render_service_executable(harness, mock_makedirs):
    with patch("shutil.copyfile") as mock_copyfile, patch("os.chmod") as mock_chmod:
        service = DPBenchmarkService()
        service.render_service_executable()
        mock_copyfile.assert_called_once_with(
            "templates/dpe_benchmark.py", "/usr/bin/dpe_benchmark.py"
        )
        mock_chmod.assert_called_once_with("/usr/bin/dpe_benchmark.py", 0o755)


def test_render_service_file(harness, mock_makedirs):
    with (
        patch("benchmark.service._render") as mock_render,
        patch("benchmark.service.daemon_reload") as mock_daemon_reload,
    ):
        service = DPBenchmarkService()
        db = DPBenchmarkExecutionModel(
            db_info=DPBenchmarkBaseDatabaseModel(
                hosts=["localhost"],
                workload_name="workload",
                db_name="test_index",
                username="user",
                password="pass",
                workload_params={},
            ),
            threads=4,
            clients=10,
            duration=60,
            extra=OpenSearchExecutionExtraConfigsModel(run_count=0, test_mode=False),
        )

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
                "workload_params": "/root/.benchmark/charmed_parameters/dpe_benchmark.json",
                "extra_labels": "",
            },
        )
        mock_daemon_reload.assert_called_once()


def test_is_running(harness, mock_makedirs):
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


def test_is_failed(harness, mock_makedirs):
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


def test_stop(harness, mock_makedirs):
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


def test_run(harness, mock_makedirs):
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


def test_unset(harness, mock_makedirs):
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

        mock_remove.assert_any_call(service.svc_path)
        mock_remove.assert_any_call(service.workload_parameter_path)
        mock_daemon_reload.assert_called_once()
