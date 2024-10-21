# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""This module contains the benchmark service."""

import os
import shutil
from typing import Any, Dict, Optional

from charms.operator_libs_linux.v1.systemd import (
    SystemdError,
    daemon_reload,
    service_failed,
    service_restart,
    service_running,
    service_stop,
)
from jinja2 import Environment, FileSystemLoader, exceptions

from benchmark.literals import (
    DPBenchmarkExecutionModel,
    DPBenchmarkServiceError,
    DPBenchmarkSystemdCommand,
)


def _render(src_template_file: str, dst_filepath: str, values: Dict[str, Any]):
    folder = os.path.dirname(src_template_file) or "templates"
    filename = os.path.basename(src_template_file)
    templates_dir = os.path.join(os.environ.get("CHARM_DIR", ""), folder)
    template_env = Environment(loader=FileSystemLoader(templates_dir))
    try:
        template = template_env.get_template(filename)
        content = template.render(values)
    except exceptions.TemplateNotFound as e:
        raise e
    # save the file in the destination
    with open(dst_filepath, "w") as f:
        f.write(content)
        os.chmod(dst_filepath, 0o640)


class DPBenchmarkService:
    """Represents the benchmark service."""

    SVC_NAME = "dpe_benchmark"
    SVC_EXECUTABLE_PATH = "/usr/bin/"
    SVC_PATH = f"/etc/systemd/system/{SVC_NAME}.service"

    @property
    def svc_path(self) -> str:
        """Returns the path to the service file."""
        return self.SVC_PATH

    @property
    def executable(self) -> str:
        """Returns the path to the service executable."""
        return self.SVC_EXECUTABLE_PATH + self.SVC_NAME + ".py"

    @property
    def workload_parameter_path(self) -> str:
        """Returns the path to the workload parameters file."""
        if not os.path.exists("/root/.benchmark/charmed_parameters"):
            os.makedirs("/root/.benchmark/charmed_parameters", exist_ok=True)
        return "/root/.benchmark/charmed_parameters/" + self.SVC_NAME + ".json"

    def render_service_executable(self) -> bool:
        """Render the benchmark service executable."""
        shutil.copyfile("templates/" + self.SVC_NAME + ".py", self.executable)
        os.chmod(self.executable, 0o755)

    def render_service_file(
        self,
        db: DPBenchmarkExecutionModel,
        labels: Optional[str] = "",
        extra_config: str | None = None,
    ) -> bool:
        """Render the systemd service file."""
        config = {
            "target_hosts": ",".join(db.db_info.hosts)
            if isinstance(db.db_info.hosts, list)
            else db.db_info.hosts,
            "workload": db.db_info.workload_name,
            "threads": db.threads,
            "clients": db.clients,
            "db_user": db.db_info.username,
            "db_password": db.db_info.password,
            "duration": db.duration,
            "workload_params": self.workload_parameter_path,
            "extra_labels": labels,
        }
        if extra_config:
            config["extra_config"] = extra_config

        _render(
            self.SVC_NAME + ".service.j2",
            self.svc_path,
            config,
        )
        return daemon_reload()

    def render_workload_parameters(
        self,
        db: DPBenchmarkExecutionModel,
        workload_name: str,
    ):
        """Renders the workload parameters file."""
        _render(
            "src/workload_parameter_templates/" + workload_name + ".json.j2",
            self.workload_parameter_path,
            db.db_info.workload_params,
        )

    def is_prepared(self) -> bool:
        """Checks if the benchmark service has passed its "prepare" status."""
        return (
            os.path.exists(self.svc_path)
            and os.path.exists(self.workload_parameter_path)
            and os.path.exists(self.executable)
        )

    def prepare(
        self,
        db: DPBenchmarkExecutionModel,
        workload_name: str,
        labels: Optional[str] = "",
        extra_config: Optional[str] = "",
    ) -> bool:
        """Prepare the benchmark service."""
        try:
            self.render_workload_parameters(
                db=db,
                workload_name=workload_name,
            )
            if not self.render_service_file(
                db=db,
                labels=labels,
                extra_config=extra_config,
            ):
                return False
        except Exception:
            return False
        return True

    def is_running(self) -> bool:
        """Checks if the benchmark service is running."""
        return self.is_prepared() and self._check_systemd(DPBenchmarkSystemdCommand.RUNNING)

    def is_stopped(self) -> bool:
        """Checks if the benchmark service has stopped."""
        return self.is_prepared() and not self.is_running() and not self.is_failed()

    def is_failed(self) -> bool:
        """Checks if the benchmark service has failed."""
        return self.is_prepared() and self._check_systemd(DPBenchmarkSystemdCommand.FAILED)

    def _check_systemd(self, cmd: DPBenchmarkSystemdCommand) -> bool:
        """Check the systemd status.

        This proxy method captures the external exception and re-raises as adequate for the benchmark.
        """
        try:
            if cmd == DPBenchmarkSystemdCommand.RESTART:
                return service_restart(self.SVC_NAME)
            if cmd == DPBenchmarkSystemdCommand.STOP:
                return service_stop(self.SVC_NAME)
            if cmd == DPBenchmarkSystemdCommand.FAILED:
                return service_failed(self.SVC_NAME)
            if cmd == DPBenchmarkSystemdCommand.RUNNING:
                return service_running(self.SVC_NAME)
            if cmd == DPBenchmarkSystemdCommand.DAEMON_RELOAD:
                return daemon_reload()
        except SystemdError as e:
            raise DPBenchmarkServiceError(e)

    def run(self) -> bool:
        """Run the benchmark service."""
        if self.is_stopped() or self.is_failed():
            return self._check_systemd(DPBenchmarkSystemdCommand.RESTART)
        return self.is_running()

    def stop(self) -> bool:
        """Stop the benchmark service."""
        if self.is_running():
            return self._check_systemd(DPBenchmarkSystemdCommand.STOP)
        return self.is_stopped()

    def unset(self) -> bool:
        """Unset the benchmark service."""
        try:
            if not (result := self.stop() and daemon_reload()):
                return False
            os.remove(self.svc_path)
            os.remove(self.workload_parameter_path)
        except Exception:
            return False
        return result

    def exec(self, command: str, extra_labels: Dict[str, str] = {}) -> None:
        """Executes a benchmark command."""
        pass
