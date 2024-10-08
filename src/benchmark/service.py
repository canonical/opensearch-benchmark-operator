# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""This module contains the benchmark service."""

from abc import abstractmethod
import os
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
    SYSTEM_SVC,
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
    """Represents the sysbench service."""

    def __init__(
        self,
        svc_name: str = SYSTEM_SVC,
    ):
        self.svc = svc_name

    @property
    def svc_path(self) -> str:
        """Returns the path to the service file."""
        return f"/etc/systemd/system/{self.svc}.service"

    def render_service_file(
        self, script: str, db_type: str, db: DPBenchmarkExecutionModel, labels: Optional[str] = ""
    ) -> bool:
        """Render the systemd service file."""
        _render(
            SYSTEM_SVC + ".j2",
            self.svc_path,
            {
                "db_driver": db_type,
                "threads": db.threads,
                "tables": db.db_info.tables,
                "db_name": db.db_info.db_name,
                "db_user": db.db_info.username,
                "db_password": db.db_info.password,
                "db_host": db.db_info.host,
                "db_port": db.db_info.port,
                "db_socket": db.db_info.unix_socket,
                "duration": db.duration,
                "script_path": script,
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
        """Checks if the sysbench service is running."""
        return self.is_prepared() and service_running(self.svc)

    def is_stopped(self) -> bool:
        """Checks if the sysbench service has stopped."""
        return (
            self.is_prepared()
            and not self.is_running()
            and not self.is_failed()
        )

    def is_failed(self) -> bool:
        """Checks if the sysbench service has failed."""
        return self.is_prepared() and service_failed(self.svc)

    def run(self) -> bool:
        """Run the sysbench service."""
        if self.is_stopped() or self.is_failed():
            return service_restart(self.svc)
        return self.is_running()

    def stop(self) -> bool:
        """Stop the sysbench service."""
        if self.is_running():
            return service_stop(self.svc)
        return self.is_stopped()

    def unset(self) -> bool:
        """Unset the sysbench service."""
        try:
            result = self.stop()
            os.remove(self.svc_path)
            return daemon_reload() and result
        except Exception:
            pass
