# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""This module contains the constants and models used by the sysbench charm."""

import os
from enum import Enum
from typing import Optional

from pydantic import BaseModel, root_validator

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


class DPBenchmarkBaseDatabaseModel(BaseModel):
    """Benchmark database model.

    Holds all the details of the sysbench database.
    """

    # List of host:port pairs to use for connecting to the database.
    hosts: Optional[list[str]]
    unix_socket: Optional[str]
    username: str
    password: str
    db_name: str
    workload_name: str
    workload_params: dict[str, str]

    @root_validator()
    @classmethod
    def validate_if_missing_params(cls, field_values):
        """Validate if missing params."""
        missing_param = []
        # Check if the required fields are present
        for f in ["username", "password", "workload_name"]:
            if f not in field_values or field_values[f] is None:
                missing_param.append(f)
        if missing_param:
            raise DPBenchmarkMissingOptionsError(f"{missing_param}")
        if os.path.exists(field_values.get("unix_socket") or ""):
            field_values["hosts"] = ""
        else:
            field_values["unix_socket"] = ""

        # Check if we have the correct endpoint
        if not field_values.get("hosts") and not field_values.get("unix_socket"):
            raise DPBenchmarkMissingOptionsError("Missing endpoint as unix_socket OR host:port")
        return field_values


class DatabaseRelationStatus(Enum):
    """Represents the different status of the database relation.

    The ERROR in this case corresponds to the case, for example, more than one
    relation exists for a given DB, or for multiple DBs.
    """

    NOT_AVAILABLE = "not_available"
    AVAILABLE = "available"
    CONFIGURED = "configured"
    ERROR = "error"


class DPBenchmarkExecutionModel(BaseModel):
    """Benchmark execution model.

    Holds all the details of the sysbench execution.
    """

    threads: int
    duration: int
    clients: int
    db_info: DPBenchmarkBaseDatabaseModel


class DPBenchmarkExecStatus(Enum):
    """Benchmark execution status.

    The state-machine is the following:
    UNSET -> PREPARED -> RUNNING -> STOPPED -> UNSET

    ERROR can be set after any state apart from UNSET, PREPARED, STOPPED.

    UNSET means waiting for prepare to be executed. STOPPED means the sysbench is ready
    but the service is not running.
    """

    UNSET = "unset"
    PREPARED = "prepared"
    RUNNING = "running"
    STOPPED = "stopped"
    ERROR = "error"


class DPBenchmarkIsInWrongStateError(DPBenchmarkError):
    """DPBenchmark is in wrong state error."""

    def __init__(self, unit_state: DPBenchmarkExecStatus, app_state: DPBenchmarkExecStatus):
        self.unit_state = unit_state
        self.app_state = app_state
        super().__init__(f"Unit state: {unit_state}, App state: {app_state}")
