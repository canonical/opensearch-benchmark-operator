#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""This connects the sysbench service to the database and the grafana agent.

The first action after installing the sysbench charm and relating it to the different
apps, is to prepare the db. The user must run the prepare action to create the database.

The prepare action will run the sysbench prepare command to create the database and, at its
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
from benchmark.constants import (
    DatabaseRelationStatus,
    DPBenchmarkExecError,
    DPBenchmarkMissingOptionsError,
)
from opensearch_relation_manager import OpenSearchDatabaseRelationManager

# Log messages can be retrieved using juju debug-log
logger = logging.getLogger(__name__)


class OpenSearchBenchmarkOperator(DPBenchmarkCharm):
    """Charm the service."""

    def __init__(self, *args):
        super().__init__(*args)
        self.framework.observe(self.on.install, self._on_install)
        self.framework.observe(self.on.config_changed, self._on_config_changed)
        self.labels = ",".join([self.model.name, self.unit.name])
        self._setup_db_relation(["opensearch"])

    @override
    def _setup_db_relation(self, relation_names: list[str]):
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
        super()._on_install(event)
        self._install_packages(["python3-pip"])

        if os.path.exists("/usr/lib/python3.12/EXTERNALLY-MANAGED"):
            os.remove("/usr/lib/python3.12/EXTERNALLY-MANAGED")
        subprocess.check_ouput("pip3 install opensearch-benchmark".split())

    @override
    def _execute_benchmark_cmd(self, extra_labels, command: str):
        """Execute the benchmark command."""
        if not (db := self.database.get_execution_options()):
            raise DPBenchmarkMissingOptionsError("Missing database options")
        try:
            output = subprocess.check_output(
                [
                    "/usr/bin/osb_svc.py",
                    f"--command={command}",
                    f"--target_hosts={db.db_info.hosts}",
                    f"--workload={db.db_info.workload}",
                    f"--threads={db.threads}",
                    f"--clients={db.clients}",
                    f"--db_user={db.db_info.username}",
                    f"--db_password={db.db_info.password}",
                    f"--duration={db.duration}",
                    f"--workload_params={db.db_info.workload_params}",
                    f"--extra_labels={extra_labels}",
                ],
                timeout=86400,
            )
        except subprocess.CalledProcessError as e:
            logger.warning(f"Process failed with: {e}")
            self.sysbench_status.set(DatabaseRelationStatus.ERROR)
            raise DPBenchmarkExecError()
        logger.debug("Sysbench output: %s", output)

    def _generate_workload_params(self):
        return {}


if __name__ == "__main__":
    ops.main(OpenSearchBenchmarkOperator)
