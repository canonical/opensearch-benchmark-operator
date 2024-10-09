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
    INDEX_NAME,
)
from benchmark.relation_manager import DatabaseRelationManager

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
        )
        self._setup_relations(["opensearch"])

    @property
    def relation_data(self):
        """Returns the relation data."""
        return list(self.relations["opensearch"].fetch_relation_data().values())[0]

    @override
    def script(self) -> Optional[str]:
        """Returns the script path for the chosen DB."""
        pass
