#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

from unittest.mock import PropertyMock, patch

import pytest
from ops.testing import Harness

from benchmark.relation_manager import (
    DatabaseRelationStatus,
    DPBenchmarkExecutionModel,
    DPBenchmarkMultipleRelationsToDBError,
)
from charm import OpenSearchBenchmarkOperator
from models import OpenSearchExecutionExtraConfigsModel


@pytest.fixture
def harness():
    harness = Harness(OpenSearchBenchmarkOperator)
    with patch("ops.model.Model.name", new_callable=PropertyMock) as mock_name:
        mock_name.return_value = "test_model"
        harness.begin()

    return harness


@pytest.fixture
def database_relation(harness):
    return harness.add_relation("opensearch", "opensearch")


def test_relation_status_not_available(harness):
    status = harness.charm.database.relation_status("opensearch")
    assert status == DatabaseRelationStatus.NOT_AVAILABLE


def test_relation_status_multiple_relations(harness, database_relation):
    harness.add_relation("opensearch", "opensearch_2")
    with pytest.raises(DPBenchmarkMultipleRelationsToDBError):
        harness.charm.database.relation_status("opensearch")


def test_relation_status_working(harness, database_relation):
    db_rel_id = database_relation
    relation = harness.model.get_relation("opensearch")
    assert not harness.charm.database._relation_has_data(relation)

    harness.update_relation_data(
        db_rel_id,
        relation.app.name,
        {},
    )
    assert not harness.charm.database._relation_has_data(relation)
    status = harness.charm.database.relation_status("opensearch")
    assert status == DatabaseRelationStatus.AVAILABLE

    harness.update_relation_data(
        db_rel_id,
        relation.app.name,
        {
            "index": "test",
            "endpoints": "localhost",
            "username": "user",
            "password": "pass",
        },
    )
    assert harness.charm.database._relation_has_data(relation)
    status = harness.charm.database.relation_status("opensearch")
    assert status == DatabaseRelationStatus.CONFIGURED


def test_relation_get_execution_options(harness, database_relation):
    db_rel_id = database_relation
    relation = harness.model.get_relation("opensearch")
    assert not harness.charm.database._relation_has_data(relation)
    harness.update_relation_data(
        db_rel_id,
        relation.app.name,
        {
            "index": "test",
            "endpoints": "localhost",
            "username": "user",
            "password": "pass",
        },
    )

    db_opts = DPBenchmarkExecutionModel(
        threads=10,
        duration=0,
        clients=8,
        db_info=harness.charm.database.get_database_options(),
        extra=OpenSearchExecutionExtraConfigsModel(run_count=0, test_mode=False),
    )

    options = harness.charm.database.get_execution_options()
    assert options == db_opts
