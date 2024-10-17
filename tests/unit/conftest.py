#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

from unittest.mock import PropertyMock, patch

import pytest
from ops.testing import Harness

from charm import OpenSearchBenchmarkOperator


@pytest.fixture
def mock_makedirs():
    with (
        patch("os.makedirs"),
        patch("os.path.exists") as mock_path_exists,
    ):
        mock_path_exists.return_value = False
        yield


@pytest.fixture
def harness():
    harness = Harness(OpenSearchBenchmarkOperator)

    with patch("ops.model.Model.name", new_callable=PropertyMock) as mock_name:
        mock_name.return_value = "test_model"
        harness.update_config({"workload_name": "nyc_taxis"})
        harness.begin()

    return harness
