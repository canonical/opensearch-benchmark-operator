#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

# import logging
# from pathlib import Path

# import pytest
# import yaml
# from ops.testing import Harness

# from charm import OpensearchDasboardsCharm

# logger = logging.getLogger(__name__)

# CONFIG = str(yaml.safe_load(Path("./config.yaml").read_text()))
# ACTIONS = str(yaml.safe_load(Path("./actions.yaml").read_text()))
# METADATA = str(yaml.safe_load(Path("./metadata.yaml").read_text()))

# OPENSEARCH_APP_NAME = "opensearch"


# @pytest.fixture
# def harness():
#     harness = Harness(OpensearchDasboardsCharm, meta=METADATA, config=CONFIG, actions=ACTIONS)

#     harness._update_config({"log_level": "INFO"})
#     harness.begin()

#     return harness


# def set_healthy_opensearch_connection(harness):
#     """Set up a functional opensearch mock."""
#     opensearch_rel_id = harness.add_relation(OPENSEARCH_REL_NAME, "opensearch")
#     harness.add_relation_unit(opensearch_rel_id, "opensearch/0")
#     harness.update_relation_data(
#         opensearch_rel_id,
#         "opensearch",
#         {"endpoints": "111.222.333.444:9200,555.666.777.888:9200"},
#     )
#     harness.update_relation_data(opensearch_rel_id, "opensearch", {"tls-ca": "<cert_data_here>"})
#     harness.update_relation_data(
#         opensearch_rel_id, f"{OPENSEARCH_APP_NAME}", {"version": "2.12.1"}
#     )

#     responses.add(
#         method="GET",
#         url="https://111.222.333.444:9200/_cluster/health",
#         status=200,
#         json={"status": "green"},
#     )
#     return opensearch_rel_id
