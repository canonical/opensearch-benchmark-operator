#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""This class implements the generic benchmark workflow.

The charm should inherit from this class and implement only the specifics for its own tool.

The main step is to execute the run action. This action renders the systemd service file and
starts the service. If the target is missing, then service errors and returns an error to
the user.

This charm should also be the main entry point to all the modelling of your benchmark tool.
"""

import logging
import os
from abc import abstractmethod
from typing import Dict, List

import ops
from charms.grafana_agent.v0.cos_agent import COSAgentProvider
from charms.operator_libs_linux.v0 import apt

from benchmark.constants import (
    COS_AGENT_RELATION,
    METRICS_PORT,
    PEER_RELATION,
    DatabaseRelationStatus,
    DPBenchmarkExecError,
    DPBenchmarkExecStatus,
    DPBenchmarkIsInWrongStateError,
    DPBenchmarkMissingOptionsError,
    DPBenchmarkMultipleRelationsToDBError,
)
from benchmark.relation_manager import DatabaseRelationManager
from benchmark.service import DPBenchmarkService

# Log messages can be retrieved using juju debug-log
logger = logging.getLogger(__name__)


class DPBenchmarkCharm(ops.CharmBase):
    """The main benchmark class."""

    def __init__(self, db_relations: list[str]):
        super().__init__(*db_relations)
        self.framework.observe(self.on.install, self._on_install)
        self.framework.observe(self.on.config_changed, self._on_config_changed)
        self.framework.observe(self.on.run_action, self.on_run_action)
        self.framework.observe(self.on.stop_action, self.on_stop_action)
        self.framework.observe(self.on.clean_action, self.on_clean_action)

        self.framework.observe(self.on[PEER_RELATION].relation_joined, self._on_peer_changed)
        self.framework.observe(self.on[PEER_RELATION].relation_changed, self._on_peer_changed)

        self.framework.observe(self.database.on.db_config_update, self._on_config_changed)
        self.framework.observe(self.database.on.update_status, self._on_update_status)

        self.database = DatabaseRelationManager(self, db_relations)
        self._grafana_agent = COSAgentProvider(
            self,
            scrape_configs=self.scrape_config,
            refresh_events=[],
        )

        self.benchmark_status = DatabaseRelationStatus(self, PEER_RELATION, self.service_cls())
        self.labels = ",".join([self.model.name, self.unit.name])

    @property
    def service_cls(self):
        """Returns the main Service class."""
        return DPBenchmarkService

    def _on_update_status(self, _):
        """Update the status of the charm."""
        self._set_status()

    def _set_status(self) -> None:
        """Recovers the sysbench status."""
        status = self.benchmark_status.check()
        if status == DPBenchmarkExecStatus.ERROR:
            self.unit.status = ops.model.BlockedStatus("Benchmark failed, please check logs")
        elif status == DPBenchmarkExecStatus.UNSET:
            self.unit.status = ops.model.ActiveStatus()
        if status == DPBenchmarkExecStatus.PREPARED:
            self.unit.status = ops.model.WaitingStatus(
                "Sysbench is prepared: execute run to start"
            )
        if status == DPBenchmarkExecStatus.RUNNING:
            self.unit.status = ops.model.ActiveStatus("Benchmark is running")
        if status == DPBenchmarkExecStatus.STOPPED:
            self.unit.status = ops.model.BlockedStatus("Benchmark is stopped after run")

    def __del__(self):
        """Set status for the operator and finishes the service.

        First, we check if there are relations with any meaningful data. If not, then
        this is the most important status to report. Then, we check the details of the
        sysbench service and the sysbench status.
        """
        try:
            status = self.database.check()
        except DPBenchmarkMultipleRelationsToDBError:
            self.unit.status = ops.model.BlockedStatus("Multiple DB relations at once forbidden!")
            return
        if status == DPBenchmarkExecStatus.NOT_AVAILABLE:
            self.unit.status = ops.model.BlockedStatus("No database relation available")
            return
        if status == DPBenchmarkExecStatus.AVAILABLE:
            self.unit.status = ops.model.WaitingStatus("Waiting on data from relation")
            return
        if status == DPBenchmarkExecStatus.ERROR:
            self.unit.status = ops.model.BlockedStatus(
                "Unexpected error with db relation: check logs"
            )
            return
        self._set_status()

    @property
    def is_tls_enabled(self):
        """Return tls status."""
        return False

    @property
    def _unit_ip(self) -> str:
        """Current unit ip."""
        return self.model.get_binding(COS_AGENT_RELATION).network.bind_address

    def _on_config_changed(self, _):
        svc = self.service_cls()
        if svc.is_running():
            svc.stop()
            if not (options := self.database.get_execution_options()):
                # Nothing to do, we can abandon this event and wait for the next changes
                return
            svc.render_service_file(
                self.database.script(), self.database.chosen_db_type(), options, labels=self.labels
            )
            svc.run()

    def _on_relation_broken(self, _):
        self.service_cls().stop()

    def scrape_config(self) -> List[Dict]:
        """Generate scrape config for the Patroni metrics endpoint."""
        return [
            {
                "metrics_path": "/metrics",
                "static_configs": [{"targets": [f"{self._unit_ip}:{METRICS_PORT}"]}],
                "tls_config": {"insecure_skip_verify": True},
                "scheme": "https" if self.is_tls_enabled else "http",
            }
        ]

    def _on_install(self, _, extra_packages: List[str] = None):
        """Installs the basic packages and python dependencies.

        No exceptions are captured as we need all the dependencies below to even start running.
        """
        self.unit.status = ops.model.MaintenanceStatus("Installing...")
        apt.update()
        apt.add_package(["python3-prometheus-client", "python3-jinja2", "unzip"] + extra_packages)

        self.service_cls().render_service_executable()
        self.unit.status = ops.model.ActiveStatus()

    def _on_peer_changed(self, _):
        """Peer relation changed."""
        if (
            not self.unit.is_leader()
            and self.benchmark_status.app_status() == DPBenchmarkExecStatus.PREPARED
            and self.benchmark_status.service_status()
            not in [DPBenchmarkExecStatus.PREPARED, DPBenchmarkExecStatus.RUNNING]
        ):
            # We need to mark this unit as prepared so we can rerun the script later
            self.benchmark_status.set(DPBenchmarkExecStatus.PREPARED)

    def check(self, event=None) -> DPBenchmarkExecStatus:
        """Wraps the status check and catches the wrong state error for processing."""
        try:
            return self.benchmark_status.check()
        except DPBenchmarkIsInWrongStateError:
            # This error means we have a new app_status change coming down via peer relation
            # and we did not receive it yet. Defer the upstream event
            if event:
                event.defer()
        return None

    def on_run_action(self, event):
        """Run benchmark action."""
        if not (status := self.check()):
            event.fail(
                f"Failed: app level reports {self.benchmark_status.app_status()} and service level reports {self.benchmark_status.service_status()}"
            )
            return
        if status == DPBenchmarkExecStatus.ERROR:
            logger.warning("Overriding ERROR status and restarting service")
        elif status not in [
            DPBenchmarkExecStatus.PREPARED,
            DPBenchmarkExecStatus.STOPPED,
        ]:
            event.fail("Failed: sysbench is not prepared")
            return

        self.unit.status = ops.model.MaintenanceStatus("Setting up benchmark")
        svc = self.service_cls()
        svc.stop()
        if not (options := self.database.get_execution_options()):
            event.fail("Failed: missing database options")
            return
        svc.render_service_file(
            self.database.script(), self.database.chosen_db_type(), options, labels=self.labels
        )
        svc.run()
        self.benchmark_status.set(DPBenchmarkExecStatus.RUNNING)
        event.set_results({"status": "running"})

    def on_stop_action(self, event):
        """Stop benchmark service."""
        if not (status := self.check()):
            event.fail(
                f"Failed: app level reports {self.benchmark_status.app_status()} and service level reports {self.benchmark_status.service_status()}"
            )
            return
        if status != DPBenchmarkExecStatus.RUNNING:
            event.fail("Failed: sysbench is not running")
            return
        svc = self.service_cls()
        svc.stop()
        self.benchmark_status.set(DPBenchmarkExecStatus.STOPPED)
        event.set_results({"status": "stopped"})

    def on_clean_action(self, event):
        """Clean the database."""
        if not self.unit.is_leader():
            event.fail("Failed: only leader can prepare the database")
            return
        if not (status := self.check()):
            event.fail(
                f"Failed: app level reports {self.benchmark_status.app_status()} and service level reports {self.benchmark_status.service_status()}"
            )
            return
        svc = self.service_cls()
        if status == DPBenchmarkExecStatus.UNSET:
            logger.warning("Sysbench units are idle, but continuing anyways")
        if status == DPBenchmarkExecStatus.RUNNING:
            logger.info("Sysbench service stopped in clean action")
            svc.stop()

        self.unit.status = ops.model.MaintenanceStatus("Cleaning up database")
        try:
            self._execute_sysbench_cmd(self.labels, "clean")
        except DPBenchmarkMissingOptionsError:
            event.fail("Failed: missing database options")
            return
        except DPBenchmarkExecError:
            event.fail("Failed: error in sysbench while executing clean")
            return
        svc.unset()
        self.benchmark_status.set(DPBenchmarkExecStatus.UNSET)

    @abstractmethod
    def _execute_benchmark_cmd(self, extra_labels, command: str):
        """Execute the benchmark command."""
        pass
        os.chmod("/usr/bin/sysbench_svc.py", 0o700)
