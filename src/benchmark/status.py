#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""This class summarizes the status of the benchmark run."""

from typing import Any

import ops

from benchmark.constants import DPBenchmarkExecStatus, DPBenchmarkIsInWrongStateError
from benchmark.service import DPBenchmarkService


class BenchmarkStatus(ops.Object):
    """Renders the benchmark status updates the relation databag."""

    def __init__(self, charm: ops.charm.CharmBase, relation: str, svc: DPBenchmarkService):
        self.charm = charm
        self.svc = svc
        self.relation = relation

    @property
    def _relation(self) -> dict[str, Any]:
        return self.charm.model.get_relation(self.relation)

    def app_status(self) -> DPBenchmarkExecStatus:
        """Returns the app status."""
        if not self._relation:
            return None
        return DPBenchmarkExecStatus(
            self._relation.data[self.charm.app].get("status", DPBenchmarkExecStatus.UNSET.value)
        )

    def unit_status(self) -> DPBenchmarkExecStatus:
        """Returns the unit status."""
        if not self._relation:
            return None
        return DPBenchmarkExecStatus(
            self._relation.data[self.charm.unit].get("status", DPBenchmarkExecStatus.UNSET.value)
        )

    def set(self, status: DPBenchmarkExecStatus) -> None:
        """Sets the status in the relation."""
        if not self._relation:
            return
        self._relation.data[self.charm.unit]["status"] = status.value

    def get(self) -> DPBenchmarkExecStatus:
        """Sets the status in the relation."""
        if not self._relation:
            return DPBenchmarkExecStatus.UNSET
        return DPBenchmarkExecStatus(
            self._relation.data[self.charm.unit].get("status", DPBenchmarkExecStatus.UNSET.value)
        )

    def _has_error_happened(self) -> bool:
        for unit in self._relation.units:
            if self._relation.data[unit].get("status") == DPBenchmarkExecStatus.ERROR.value:
                return True
        return (
            self.unit_status() == DPBenchmarkExecStatus.ERROR
            or self.app_status() == DPBenchmarkExecStatus.ERROR
        )

    def service_status(self) -> DPBenchmarkExecStatus:
        """Returns the status of the sysbench service.

        This method will be relevant in two moments: (1) deciding if we can execute a given action
        and (2) deciding the final status to report back to the user.
        """
        if not self.svc.is_prepared() and self.get() == DPBenchmarkExecStatus.UNSET:
            return DPBenchmarkExecStatus.UNSET
        # Running and failed take priority: we do not check self.get()
        # as they reveal a lot of information about current benchmark status
        if self.svc.is_failed():
            return DPBenchmarkExecStatus.ERROR
        if self.svc.is_running():
            return DPBenchmarkExecStatus.RUNNING

        # We are only stopped if we are marked as stopped and actually set as stopped in our state
        if self.svc.is_stopped() and self.get() == DPBenchmarkExecStatus.STOPPED:
            return DPBenchmarkExecStatus.STOPPED
        return DPBenchmarkExecStatus.PREPARED

    def check(self) -> DPBenchmarkExecStatus:
        """Implements the state machine.

        This charm will also update the databag accordingly. It is built of three
        different data sources: this unit last status (from relation), app status and
        the current status of the sysbench service.
        """
        if not self.app_status() or not self.unit_status() or self.charm.app.planned_units() <= 1:
            # Trivial case, the cluster does not exist. Return the service status
            return self.service_status()

        if self._has_error_happened():
            return DPBenchmarkExecStatus.ERROR

        if self.charm.unit.is_leader():
            # Either we are waiting for PREPARE to happen, or it has happened, as
            # the prepare command runs synchronously with the charm. Check if the
            # target exists:
            self.set(self.service_status())
            return self.service_status()

        # Now, we need to execute the unit state
        self.set(self.service_status())
        # If we have a failure, then we should react to it
        if self.service_status() != self.app_status():
            raise DPBenchmarkIsInWrongStateError(self.service_status(), self.app_status())
        return self.service_status()
