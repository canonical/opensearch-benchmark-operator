# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""This module contains the constants and models used by the sysbench charm."""

from enum import Enum

from benchmark.constants import DPBenchmarkExecutionExtraConfigsModel

VALID_LOG_LEVELS = ["info", "debug", "warning", "error", "critical"]


INDEX_NAME = "benchmark_index"


METRICS_PORT = 8088
COS_AGENT_RELATION = "cos-agent"
PEER_RELATION = "benchmark-peer"


class DPBenchmarkError(Exception):
    """Benchmark error."""


class DPBenchmarkExecError(DPBenchmarkError):
    """Sysbench failed to execute a command."""


class DPBenchmarkMultipleRelationsToDBError(DPBenchmarkError):
    """Multiple relations to the same or multiple DBs exist."""


class DPBenchmarkExecFailedError(DPBenchmarkError):
    """Sysbench execution failed error."""


class DPBenchmarkMissingOptionsError(DPBenchmarkError):
    """Sysbench missing options error."""


class DatabaseRelationStatus(Enum):
    """Represents the different status of the database relation.

    The ERROR in this case corresponds to the case, for example, more than one
    relation exists for a given DB, or for multiple DBs.
    """

    NOT_AVAILABLE = "not_available"
    AVAILABLE = "available"
    CONFIGURED = "configured"
    ERROR = "error"


class OpenSearchExecutionExtraConfigsModel(DPBenchmarkExecutionExtraConfigsModel):
    """OpenSearch execution model's extra configuration."""

    run_count: int = 0
    test_mode: bool = False

    def __str__(self):
        """Return the string representation of the model."""
        if self.test_mode:
            return f"{super().__str__()} --test_mode"
        return super().__str__()
