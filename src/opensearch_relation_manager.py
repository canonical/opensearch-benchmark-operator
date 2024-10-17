# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""This module provides a single API set for database management."""

import logging
from typing import List, Optional

from charms.data_platform_libs.v0.data_interfaces import OpenSearchRequires
from ops.charm import CharmBase, CharmEvents
from ops.framework import EventBase, EventSource
from overrides import override

from benchmark.constants import (
    DPBenchmarkBaseDatabaseModel,
    DPBenchmarkExecutionExtraConfigsModel,
    DPBenchmarkExecutionModel,
)
from benchmark.relation_manager import DatabaseRelationManager
from models import (
    INDEX_NAME,
    OpenSearchExecutionExtraConfigsModel,
)

logger = logging.getLogger(__name__)


class DatabaseConfigUpdateNeededEvent(EventBase):
    """informs the charm that we have an update in the DB config."""


class DatabaseManagerEvents(CharmEvents):
    """Events used by the Database Relation Manager to communicate with the charm."""

    db_config_update = EventSource(DatabaseConfigUpdateNeededEvent)


class OpenSearchDatabaseRelationManager(DatabaseRelationManager):
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
        self._setup_relations(["opensearch"])

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
