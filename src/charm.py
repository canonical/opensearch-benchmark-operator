#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""This connects the benchmark service to the database and the grafana agent.

The first action after installing the benchmark charm and relating it to the different
apps, is to prepare the db. The user must run the prepare action to create the database.

The prepare action will run the benchmark prepare command to create the database and, at its
end, it sets a systemd target informing the service is ready.

The next step is to execute the run action. This action renders the systemd service file and
starts the service. If the target is missing, then service errors and returns an error to
the user.
"""

import logging
import os
import subprocess

import ops
from overrides import override

from benchmark.benchmark_charm import DPBenchmarkCharm
from opensearch_relation_manager import OpenSearchDatabaseRelationManager

# Log messages can be retrieved using juju debug-log
logger = logging.getLogger(__name__)


class OpenSearchBenchmarkOperator(DPBenchmarkCharm):
    """Charm the service."""

    def __init__(self, *args):
        super().__init__(*args)
        self.framework.observe(self.on.install, self._on_install)
        self.framework.observe(self.on.config_changed, self._on_config_changed)
        self.labels = ",".join([self.model.name, self.unit.name.replace("/", "-")])
        self.setup_db_relation(["opensearch"])

    @override
    def list_supported_workloads(self) -> list[str]:
        """List the supported workloads."""
        return [
            ".".join(name.split(".")[:-2])
            for name in os.listdir("src/workload_parameter_templates")
        ]

    @override
    def setup_db_relation(self, relation_names: list[str]):
        """Setup the database relation."""
        self.database = OpenSearchDatabaseRelationManager(
            self,
            relation_names,
            workload_name=self.config["workload_name"],
            workload_params=self._generate_workload_params(),
        )
        self.framework.observe(self.database.on.db_config_update, self._on_config_changed)

    @override
    def _on_install(self, event):
        self.unit.status = ops.model.MaintenanceStatus("Installing...")
        self._install_packages(["python3-pip", "python3-prometheus-client"])

        if os.path.exists("/usr/lib/python3.12/EXTERNALLY-MANAGED"):
            os.remove("/usr/lib/python3.12/EXTERNALLY-MANAGED")
        try:
            # Most recent versions of opensearch-benchmark require a newer version of jinja2
            # We make sure it is uninstalled before we can install OSB
            subprocess.check_output("apt purge -y python3-jinja2".split())
        except Exception:
            pass
        subprocess.check_output("pip3 install opensearch-benchmark".split())

        self.SERVICE_CLS().render_service_executable()
        self.unit.status = ops.model.ActiveStatus()

    @override
    def execute_benchmark_cmd(self, extra_labels, command: str):
        """Execute the benchmark command."""
        # There is no reason to execute any other command besides run for OSB.
        pass

    def _generate_workload_params(self):
        return {}


if __name__ == "__main__":
    ops.main(OpenSearchBenchmarkOperator)
