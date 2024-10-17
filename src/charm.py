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
from typing import List, Optional

import ops
from charms.data_platform_libs.v0.data_interfaces import OpenSearchRequires
from ops.charm import CharmBase, EventBase
from ops.model import ActiveStatus, MaintenanceStatus
from overrides import override

from benchmark.base_charm import DPBenchmarkCharmBase
from benchmark.events.db import DatabaseRelationHandler
from benchmark.literals import (
    DPBenchmarkBaseDatabaseModel,
    DPBenchmarkExecutionExtraConfigsModel,
    DPBenchmarkExecutionModel,
)
from literals import INDEX_NAME, OpenSearchExecutionExtraConfigsModel

# Log messages can be retrieved using juju debug-log
logger = logging.getLogger(__name__)


class OpenSearchDatabaseRelationManager(DatabaseRelationHandler):
    """Listens to all the DB-related events and react to them.

    This class will provide the charm with the necessary data to connect to the DB as
    well as the current relation status.
    """

    DATABASE_KEY = "index"

    def __init__(
        self,
        charm: CharmBase,
        relation_names: List[str] | None,
        *,
        workload_name: str = None,
        workload_params: dict[str, str] = {},
    ):
        super().__init__(
            charm, ["opensearch"], workload_name=workload_name, workload_params=workload_params
        )
        self.relations["opensearch"] = OpenSearchRequires(
            charm,
            "opensearch",
            INDEX_NAME,
            extra_user_roles="admin",
        )

    @property
    def relation_data(self):
        """Returns the relation data."""
        return list(self.relations["opensearch"].fetch_relation_data().values())[0]

    @override
    def get_execution_options(
        self,
        extra_config: DPBenchmarkExecutionExtraConfigsModel = DPBenchmarkExecutionExtraConfigsModel(),
    ) -> Optional[DPBenchmarkExecutionModel]:
        """Returns the execution options."""
        return super().get_execution_options(
            extra_config=OpenSearchExecutionExtraConfigsModel(
                run_count=self.charm.config.get("run_count", 0),
                test_mode=self.charm.config.get("test_mode", False),
            )
        )

    @override
    def get_database_options(self) -> DPBenchmarkBaseDatabaseModel:
        """Returns the database options."""
        endpoints = self.relation_data.get("endpoints")

        unix_socket = None
        if endpoints.startswith("file://"):
            unix_socket = endpoints[7:]

        return DPBenchmarkBaseDatabaseModel(
            hosts=[f"https://{url}" for url in endpoints.split(",")],
            unix_socket=unix_socket,
            username=self.relation_data.get("username"),
            password=self.relation_data.get("password"),
            db_name=self.relation_data.get(self.DATABASE_KEY),
            workload_name=self.workload_name,
            workload_params=self.workload_params,
        )


class OpenSearchBenchmarkOperator(DPBenchmarkCharmBase):
    """Charm the service."""

    def __init__(self, *args):
        db_relation_names = ["opensearch"]
        super().__init__(*args, db_relation_names=db_relation_names)
        self.framework.observe(self.on.install, self._on_install)
        self.framework.observe(self.on.config_changed, self._on_config_changed)
        self.labels = ",".join([self.model.name, self.unit.name.replace("/", "-")])
        self.database = OpenSearchDatabaseRelationManager(
            self,
            db_relation_names,
            workload_name=self.config["workload_name"],
            workload_params=self._generate_workload_params(),
        )
        self.framework.observe(self.database.on.db_config_update, self._on_config_changed)

    @override
    def _on_install(self, event: EventBase) -> None:
        super()._on_install(event)
        self.unit.status = MaintenanceStatus("Installing...")
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

        self.service.render_service_executable()
        self.unit.status = ActiveStatus()

    def _generate_workload_params(self):
        return {}


if __name__ == "__main__":
    ops.main(OpenSearchBenchmarkOperator)
