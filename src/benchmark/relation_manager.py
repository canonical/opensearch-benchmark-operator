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

from benchmark.constants import (
    DatabaseRelationStatus,
    DPBenchmarkBaseDatabaseModel,
    DPBenchmarkExecutionModel,
    DPBenchmarkMultipleRelationsToDBError,
)
from ops.charm import CharmBase, CharmEvents
from ops.framework import EventBase, EventSource, Object
from ops.model import ModelError, Relation

logger = logging.getLogger(__name__)


class DatabaseConfigUpdateNeededEvent(EventBase):
    """informs the charm that we have an update in the DB config."""


class DatabaseManagerEvents(CharmEvents):
    """Events used by the Database Relation Manager to communicate with the charm."""

    db_config_update = EventSource(DatabaseConfigUpdateNeededEvent)


class DatabaseRelationManager(Object):
    """Listens to all the DB-related events and react to them.

    This class will provide the charm with the necessary data to connect to the DB as
    well as the current relation status.
    """

    on = DatabaseManagerEvents()  # pyright: ignore [reportGeneralTypeIssues]

    def __init__(self, charm: CharmBase, relation_names: List[str]):
        super().__init__(charm, None)
        self.charm = charm
        self.relations = {}
        for rel in relation_names:
            self.framework.observe(
                getattr(self.relations[rel].on, "endpoints_changed"),
                self._on_endpoints_changed,
            )
            self.framework.observe(self.charm.on[rel].relation_broken, self._on_endpoints_changed)

    def relation_status(self, relation_name) -> DatabaseRelationStatus:
        """Returns the current relation status."""
        relation = self.charm.model.relations[relation_name]
        if len(relation) > 1:
            raise DPBenchmarkMultipleRelationsToDBError()
        elif len(relation) == 0:
            return DatabaseRelationStatus.NOT_AVAILABLE
        if self._is_relation_active(relation[0]):
            # Relation exists and we have some data
            # Try to create an options object and see if it fails
            try:
                for rel, requirer in self.relations.items():
                    if self.relation_status(rel) == DatabaseRelationStatus.CONFIGURED:
                        DatabaseRelationStatus(self.charm, requirer).get_database_options()
                        # We've managed to create at least one database relation, leave the loop
                        break
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

    def _is_relation_active(self, relation: Relation):
        """Whether the relation is active based on contained data."""
        try:
            _ = repr(relation.data)
            return True
        except (RuntimeError, ModelError) as e:
            logger.debug("Failed relation status check %s" % e)
            return False

    def get_db_config(self) -> Optional[DPBenchmarkBaseDatabaseModel]:
        """Checks each relation: if there is a valid relation, build its options and return.

        This class does not raise: MultipleRelationsToSameDBTypeError. It either returns the
        data of the first valid relation or just returns None. The error above must be used
        to manage the final status of the charm only.
        """
        for rel, requirer in self.relations.items():
            if self.relation_status(rel) == DatabaseRelationStatus.CONFIGURED:
                return DatabaseRelationStatus(self.charm, requirer).get_database_options()

        return None

    def _on_endpoints_changed(self, _):
        """Handles the endpoints_changed event."""
        self.on.db_config_update.emit()

    def get_execution_options(self) -> Optional[DPBenchmarkExecutionModel]:
        """Returns the execution options."""
        if not (db := self.get_db_config()):
            # It means we are not yet ready. Return None
            # This check also serves to ensure we have only one valid relation at the time
            return None
        return DPBenchmarkExecutionModel(
            threads=self.charm.config.get("threads"),
            duration=self.charm.config.get("duration"),
            db_info=db,
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

    @abstractmethod
    def script(self) -> Optional[str]:
        """Returns the script path for the chosen DB."""
        pass
