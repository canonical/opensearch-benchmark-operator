# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""This module abstracts the different DBs and provide a single API set.

The DatabaseRelationManager listens to DB events and manages the relation lifecycles.
The charm interacts with the manager and requests data + listen to some key events such
as changes in the configuration.
"""

import logging
from abc import abstractmethod
from typing import List, Optional

from ops.charm import CharmBase, CharmEvents
from ops.framework import EventBase, EventSource, Object
from ops.model import Relation

from benchmark.core.models import (
    DatabaseState,
    DatabaseRelationStatus,
    DPBenchmarkBaseDatabaseModel,
    DPBenchmarkExecutionExtraConfigsModel,
    DPBenchmarkExecutionModel,
    DPBenchmarkMultipleRelationsToDBError,
)

logger = logging.getLogger(__name__)


class DatabaseConfigUpdateNeededEvent(EventBase):
    """informs the charm that we have an update in the DB config."""


class DatabaseHandlerEvents(CharmEvents):
    """Events used by the Database Relation Manager to communicate with the charm."""

    db_config_update = EventSource(DatabaseConfigUpdateNeededEvent)


class DatabaseRelationHandler(Object):
    """Listens to all the DB-related events and react to them.

    This class will provide the charm with the necessary data to connect to the DB as
    well as the current relation status.
    """

    on = DatabaseHandlerEvents()  # pyright: ignore [reportGeneralTypeIssues]

    def __init__(
        self,
        charm: CharmBase,
        relation_names: List[str] | None,
        *,
        workload_name: str = None,
        workload_params: dict[str, str] = {},
    ):
        super().__init__(charm, None)
        self.database_key = "database"
        self.charm = charm
        self.workload_name = workload_name
        self.workload_params = workload_params
        self.relations = {}
        for rel in relation_names:
            self.framework.observe(self.charm.on[rel].relation_joined, self._on_endpoints_changed)
            self.framework.observe(self.charm.on[rel].relation_changed, self._on_endpoints_changed)
            self.framework.observe(self.charm.on[rel].relation_broken, self._on_endpoints_changed)
            self.relations[rel] = DatabaseState(rel, self.charm.model.relations[rel])

    def relation_status(self, relation_name) -> DatabaseRelationStatus:
        """Returns the current relation status."""
        relation = 
        if len(relation) > 1:
            raise DPBenchmarkMultipleRelationsToDBError()
        elif len(relation) == 0:
            return DatabaseRelationStatus.NOT_AVAILABLE
        if self._relation_has_data(relation[0]):
            # Relation exists and we have some data
            # Try to create an options object and see if it fails
            try:
                self.get_database_options()
            except Exception as e:
                logger.debug("Failed relation options check %s" % e)
            else:
                # We have data to build the config object
                return DatabaseRelationStatus.CONFIGURED
        return DatabaseRelationStatus.AVAILABLE

    def check(self) -> DatabaseRelationStatus:
        """Returns the current status of all the relations, aggregated."""
        status = DatabaseRelationStatus.NOT_AVAILABLE
        for rel in self.relations.keys():
            if self.relation_status(rel) != DatabaseRelationStatus.NOT_AVAILABLE:
                if status != DatabaseRelationStatus.NOT_AVAILABLE:
                    # It means we have the same relation to more than one DB
                    raise DPBenchmarkMultipleRelationsToDBError()
                status = self.relation_status(rel)
        return status

    def _relation_has_data(self, relation: Relation) -> bool:
        """Whether the relation is active based on contained data."""
        return relation.data.get(relation.app, {}) != {}

    def _on_endpoints_changed(self, event: EventBase) -> None:
        """Handles the endpoints_changed event."""
        self.on.db_config_update.emit()

    def get_execution_options(
        self,
        extra_config: DPBenchmarkExecutionExtraConfigsModel = DPBenchmarkExecutionExtraConfigsModel(),
    ) -> Optional[DPBenchmarkExecutionModel]:
        """Returns the execution options."""
        if not (db := self.get_database_options()):
            # It means we are not yet ready. Return None
            # This check also serves to ensure we have only one valid relation at the time
            return None
        return DPBenchmarkExecutionModel(
            threads=self.charm.config.get("threads"),
            duration=self.charm.config.get("duration"),
            clients=self.charm.config.get("clients"),
            db_info=db,
            extra=extra_config,
        )

    def chosen_db_type(self) -> Optional[str]:
        """Returns the chosen DB type."""
        for rel in self.relations.keys():
            if self.relation_status(rel) in [
                DatabaseRelationStatus.AVAILABLE,
                DatabaseRelationStatus.CONFIGURED,
            ]:
                return rel
        return None

    @property
    @abstractmethod
    def relation_data(self):
        """Returns the relation data."""
        pass
