# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""This module provides a single API set for database management."""

import logging
from abc import abstractmethod
from typing import List, Optional

from charms.data_platform_libs.v0.data_interfaces import OpenSearchRequires
from benchmark.constants import (
    INDEX_NAME,
)
from ops.charm import CharmBase, CharmEvents
from ops.framework import EventBase, EventSource

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

    on = DatabaseManagerEvents()  # pyright: ignore [reportGeneralTypeIssues]

    def __init__(self, charm: CharmBase, relation_names: List[str]):
        super().__init__(charm, None, relation_names)
        for rel in relation_names:
            self.relations[rel] = OpenSearchRequires(
                self.charm,
                rel,
                INDEX_NAME,
                external_node_connectivity=self.charm.config.get(
                    "request-external-connectivity", False
                ),
            )
            self.framework.observe(
                getattr(self.relations[rel].on, "endpoints_changed"),
                self._on_endpoints_changed,
            )
            self.framework.observe(self.charm.on[rel].relation_broken, self._on_endpoints_changed)

    @abstractmethod
    def script(self) -> Optional[str]:
        """Returns the script path for the chosen DB."""
        pass
