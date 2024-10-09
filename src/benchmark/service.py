# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""This module contains the benchmark service."""

import os
import shutil
from abc import abstractmethod
from typing import Any, Dict, Optional

from charms.operator_libs_linux.v1.systemd import (
    daemon_reload,
    service_failed,
    service_restart,
    service_running,
    service_stop,
)
from jinja2 import Environment, FileSystemLoader, exceptions

from .constants import (
    DPBenchmarkExecutionModel,
)


def _render(src_template_file: str, dst_filepath: str, values: Dict[str, Any]):
    templates_dir = os.path.join(os.environ.get("CHARM_DIR", ""), "templates")
    template_env = Environment(loader=FileSystemLoader(templates_dir))
    try:
        template = template_env.get_template(src_template_file)
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

    def render_service_executable(self) -> bool:
        """Render the benchmark service executable."""
        shutil.copyfile(
            "templates/" + self.SVC_NAME + ".py", self.SVC_EXECUTABLE_PATH + self.SVC_NAME + ".py"
        )
        os.chmod(self.SVC_EXECUTABLE_PATH + self.SVC_NAME + ".py", 0o755)

    def render_service_file(
        self, db: DPBenchmarkExecutionModel, labels: Optional[str] = ""
    ) -> bool:
        """Render the systemd service file."""
        _render(
            self.SVC_NAME + ".service.j2",
            self.svc_path,
            {
                "target_hosts": ",".join(db.db_info.hosts)
                if isinstance(db.db_info.hosts, list)
                else db.db_info.hosts,
                "workload": db.db_info.workload_name,
                "threads": db.threads,
                "clients": db.clients,
                "db_user": db.db_info.username,
                "db_password": db.db_info.password,
                "duration": db.duration,
                "workload_params": db.db_info.workload_params,
                "extra_labels": labels,
            },
        )
        return daemon_reload()

    def is_prepared(self) -> bool:
        """Checks if the benchmark service has passed its "prepare" status."""
        return os.path.exists(self.svc_path)

    @abstractmethod
    def prepare(self) -> bool:
        """Prepare the benchmark service."""
        pass

    def is_running(self) -> bool:
        """Checks if the benchmark service is running."""
        return self.is_prepared() and service_running(self.SVC_NAME)

    def is_stopped(self) -> bool:
        """Checks if the benchmark service has stopped."""
        return self.is_prepared() and not self.is_running() and not self.is_failed()

    def is_failed(self) -> bool:
        """Checks if the benchmark service has failed."""
        return self.is_prepared() and service_failed(self.SVC_NAME)

    def run(self) -> bool:
        """Run the benchmark service."""
        if self.is_stopped() or self.is_failed():
            return service_restart(self.SVC_NAME)
        return self.is_running()

    def stop(self) -> bool:
        """Stop the benchmark service."""
        if self.is_running():
            return service_stop(self.SVC_NAME)
        return self.is_stopped()

    def unset(self) -> bool:
        """Unset the benchmark service."""
        try:
            result = self.stop()
            os.remove(self.svc_path)
            return daemon_reload() and result
        except Exception:
            pass
