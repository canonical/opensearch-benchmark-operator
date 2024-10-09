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
from benchmark.status import BenchmarkStatus

# Log messages can be retrieved using juju debug-log
logger = logging.getLogger(__name__)


class DPBenchmarkCharm(ops.CharmBase):
    """The main benchmark class."""

    SERVICE_CLS = DPBenchmarkService

    def __init__(self, *args):
        super().__init__(*args)
        self.framework.observe(self.on.install, self._on_install)
        self.framework.observe(self.on.config_changed, self._on_config_changed)
        self.framework.observe(self.on.prepare_action, self.on_prepare_action)
        self.framework.observe(self.on.run_action, self.on_run_action)
        self.framework.observe(self.on.stop_action, self.on_stop_action)
        self.framework.observe(self.on.clean_action, self.on_clean_action)
        self.framework.observe(self.on.update_status, self._on_update_status)

        self.framework.observe(self.on[PEER_RELATION].relation_joined, self._on_peer_changed)
        self.framework.observe(self.on[PEER_RELATION].relation_changed, self._on_peer_changed)

        self._grafana_agent = COSAgentProvider(
            self,
            relation_name=COS_AGENT_RELATION,
            metrics_endpoints=[],
            refresh_events=[
                self.on[PEER_RELATION].relation_joined,
                self.on[PEER_RELATION].relation_changed,
                self.on.config_changed,
            ],
            scrape_configs=self.scrape_config,
        )
        self.database = None
        self.benchmark_status = BenchmarkStatus(self, PEER_RELATION, self.SERVICE_CLS())
        self.labels = ",".join([self.model.name, self.unit.name])

    def _setup_db_relation(self, relation_names: List[str]):
        """Setup the database relation."""
        self.database = DatabaseRelationManager(self, relation_names)
        self.framework.observe(self.database.on.db_config_update, self._on_config_changed)

    def _on_update_status(self, _):
        """Update the status of the charm."""
        self._set_status()

    def _set_status(self) -> None:
        """Recovers the benchmark status."""
        status = self.benchmark_status.check()
        if status == DPBenchmarkExecStatus.ERROR:
            self.unit.status = ops.model.BlockedStatus("Benchmark failed, please check logs")
        elif status == DPBenchmarkExecStatus.UNSET:
            self.unit.status = ops.model.ActiveStatus()
        if status == DPBenchmarkExecStatus.PREPARED:
            self.unit.status = ops.model.WaitingStatus(
                "Benchmark is prepared: execute run to start"
            )
        if status == DPBenchmarkExecStatus.RUNNING:
            self.unit.status = ops.model.ActiveStatus("Benchmark is running")
        if status == DPBenchmarkExecStatus.STOPPED:
            self.unit.status = ops.model.BlockedStatus("Benchmark is stopped after run")

    def __del__(self):
        """Set status for the operator and finishes the service.

        First, we check if there are relations with any meaningful data. If not, then
        this is the most important status to report. Then, we check the details of the
        benchmark service and the benchmark status.
        """
        try:
            status = self.database.check()
        except DPBenchmarkMultipleRelationsToDBError:
            self.unit.status = ops.model.BlockedStatus("Multiple DB relations at once forbidden!")
            return
        if status == DatabaseRelationStatus.NOT_AVAILABLE:
            self.unit.status = ops.model.BlockedStatus("No database relation available")
            return
        if status == DatabaseRelationStatus.AVAILABLE:
            self.unit.status = ops.model.WaitingStatus("Waiting on data from relation")
            return
        if status == DatabaseRelationStatus.ERROR:
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
        svc = self.SERVICE_CLS()
        if svc.is_running():
            svc.stop()
            if not (options := self.database.get_execution_options()):
                # Nothing to do, we can abandon this event and wait for the next changes
                # Status will be handled at the end of the event
                return
            svc.render_service_file(options, labels=self.labels)
            svc.run()

    def _on_relation_broken(self, _):
        self.SERVICE_CLS().stop()

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

    def _install_packages(self, extra_packages: List[str] = []):
        """Install the packages needed for the benchmark."""
        self.unit.status = ops.model.MaintenanceStatus("Installing apt packages...")
        apt.update()
        apt.add_package(extra_packages or [])
        if extra_packages:
            apt.add_package(extra_packages)
        self.unit.status = ops.model.ActiveStatus()

    def _on_install(self, event):
        """Installs the basic packages and python dependencies.

        No exceptions are captured as we need all the dependencies below to even start running.
        """
        self.unit.status = ops.model.MaintenanceStatus("Installing...")
        self._install_packages(["python3-prometheus-client", "python3-jinja2", "unzip"])

        self.SERVICE_CLS().render_service_executable()
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

    def on_prepare_action(self, event):
        """Prepare the database.

        There are two steps: the actual prepare command and setting a target to inform the
        prepare was successful.
        """
        if not self.unit.is_leader():
            event.fail("Failed: only leader can prepare the database")
            return
        if not (status := self.check()):
            event.fail(
                f"Failed: app level reports {self.benchmark_status.app_status()} and service level reports {self.benchmark_status.service_status()}"
            )
            return
        if status != DPBenchmarkExecStatus.UNSET:
            event.fail(
                "Failed: benchmark is already prepared, stop and clean up the cluster first"
            )

        self.unit.status = ops.model.MaintenanceStatus("Running prepare command...")
        try:
            self._execute_benchmark_cmd(self.labels, "prepare")
        except DPBenchmarkMultipleRelationsToDBError:
            event.fail("Failed: missing database options")
            return
        except DPBenchmarkExecError:
            event.fail("Failed: error in benchmark while executing prepare")
            return
        if not self._setup_service():
            event.fail("Failed: missing database options")
        event.set_results({"status": "prepared"})

    def _setup_service(self) -> bool:
        self.unit.status = ops.model.MaintenanceStatus("Setting up benchmark")
        if not (options := self.database.get_execution_options()):
            return False
        self.SERVICE_CLS().render_service_file(options, labels=self.labels)
        self.benchmark_status.set(DPBenchmarkExecStatus.PREPARED)

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
            event.fail("Failed: benchmark is not prepared")
            return

        svc = self.SERVICE_CLS()
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
            event.fail("Failed: benchmark is not running")
            return
        svc = self.SERVICE_CLS()
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
        svc = self.SERVICE_CLS()
        if status == DPBenchmarkExecStatus.UNSET:
            logger.warning("benchmark units are idle, but continuing anyways")
        if status == DPBenchmarkExecStatus.RUNNING:
            logger.info("benchmark service stopped in clean action")
            svc.stop()

        self.unit.status = ops.model.MaintenanceStatus("Cleaning up database")
        try:
            self._execute_benchmark_cmd(self.labels, "clean")
        except DPBenchmarkMissingOptionsError:
            event.fail("Failed: missing database options")
            return
        except DPBenchmarkExecError:
            event.fail("Failed: error in benchmark while executing clean")
            return
        svc.unset()
        self.benchmark_status.set(DPBenchmarkExecStatus.UNSET)

    @abstractmethod
    def _execute_benchmark_cmd(self, extra_labels, command: str):
        """Execute the benchmark command."""
        pass
