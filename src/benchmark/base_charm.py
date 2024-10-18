# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""This class implements the default benchmark workflow.

It is a functional charm that connects the benchmark service to the database and the grafana agent.

The charm should inherit from this class and implement only the specifics for its own tool.

The main step is to execute the run action. This action renders the systemd service file and
starts the service. If the target is missing, then service errors and returns an error to
the user.

This charm should also be the main entry point to all the modelling of your benchmark tool.
"""

import logging
import os
from typing import Dict, List

import ops
from charms.grafana_agent.v0.cos_agent import COSAgentProvider
from charms.operator_libs_linux.v0 import apt
from ops.framework import EventBase
from ops.model import ActiveStatus, BlockedStatus, MaintenanceStatus, WaitingStatus

from benchmark.core.state import BenchmarkState
from benchmark.events.db import DatabaseRelationHandler
from benchmark.literals import (
    COS_AGENT_RELATION,
    METRICS_PORT,
    PEER_RELATION,
    DatabaseRelationStatus,
    DPBenchmarkError,
    DPBenchmarkExecError,
    DPBenchmarkExecStatus,
    DPBenchmarkIsInWrongStateError,
    DPBenchmarkMissingOptionsError,
    DPBenchmarkMultipleRelationsToDBError,
    DPBenchmarkServiceError,
    DPBenchmarkStatusError,
    DPBenchmarkUnitNotReadyError,
)
from benchmark.managers.service import DPBenchmarkService

# Log messages can be retrieved using juju debug-log
logger = logging.getLogger(__name__)


class DPBenchmarkCharmBase(ops.CharmBase):
    """The base benchmark class."""

    def __init__(self, *args, db_relation_names: List[str] = None):
        super().__init__(*args)
        self.service = DPBenchmarkService()

        self.framework.observe(self.on.install, self._on_install)
        self.framework.observe(self.on.config_changed, self._on_config_changed)
        self.framework.observe(self.on.prepare_action, self.on_prepare_action)
        self.framework.observe(self.on.run_action, self.on_run_action)
        self.framework.observe(self.on.stop_action, self.on_stop_action)
        self.framework.observe(self.on.clean_action, self.on_clean_action)
        self.framework.observe(self.on.update_status, self._on_update_status)
        self.framework.observe(self.on.list_workloads_action, self.on_list_workloads_action)

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
        self.benchmark_status = BenchmarkState(self, PEER_RELATION, self.service)
        self.labels = f"{self.model.name},{self.unit.name}"

        self.database = DatabaseRelationHandler(self, db_relation_names)
        self.framework.observe(self.database.on.db_config_update, self._on_config_changed)

    def _on_update_status(self, event: EventBase) -> None:
        """Set status for the operator and finishes the service.

        First, we check if there are relations with any meaningful data. If not, then
        this is the most important status to report. Then, we check the details of the
        benchmark service and the benchmark status.
        """
        try:
            status = self.database.check()
        except DPBenchmarkMultipleRelationsToDBError:
            self.unit.status = BlockedStatus("Multiple DB relations at once forbidden!")
            return
        if status == DatabaseRelationStatus.NOT_AVAILABLE:
            self.unit.status = BlockedStatus("No database relation available")
            return
        if status == DatabaseRelationStatus.AVAILABLE:
            self.unit.status = WaitingStatus("Waiting on data from relation")
            return
        if status == DatabaseRelationStatus.ERROR:
            self.unit.status = BlockedStatus("Unexpected error with db relation: check logs")
            return
        self._set_status()

    def _set_status(self) -> None:
        """Recovers the benchmark status."""
        if not (status := self.check()):
            self.unit.status = WaitingStatus("Benchmark not ready")
            return

        if status == DPBenchmarkExecStatus.ERROR:
            self.unit.status = BlockedStatus("Benchmark failed, please check logs")
        elif status == DPBenchmarkExecStatus.UNSET:
            self.unit.status = ActiveStatus()
        if status == DPBenchmarkExecStatus.PREPARED:
            self.unit.status = WaitingStatus("Benchmark is prepared: execute run to start")
        if status == DPBenchmarkExecStatus.RUNNING:
            self.unit.status = ActiveStatus("Benchmark is running")
        if status == DPBenchmarkExecStatus.STOPPED:
            self.unit.status = BlockedStatus("Benchmark is stopped after run")

    def _unit_ip(self) -> str:
        """Current unit ip."""
        return self.model.get_binding(PEER_RELATION).network.bind_address

    def _on_config_changed(self, event: EventBase) -> None:
        """Config changed event."""
        # We need to narrow the options of workload_name to the supported ones
        if self.config.get("workload_name", "nyc_taxis") not in self.supported_workloads():
            self.unit.status = BlockedStatus("Unsupported workload")
            logger.error(f"Unsupported workload {self.config.get('workload_name', 'nyc_taxis')}")
            return
        try:
            # First, we check if the status of the service
            if not (status := self.check()) or status == DPBenchmarkExecStatus.UNSET:
                logger.debug(
                    "The config changed happened too early in the lifecycle, nothing to do"
                )
                raise DPBenchmarkError()
            if not self.stop():
                logger.warning("Config changed: tried stopping the service but returned False")
                raise DPBenchmarkError()
            if not self.clean_up():
                logger.info("Config changed: tried cleaning up the service but returned False")
                raise DPBenchmarkError()
            if not self.prepare():
                logger.info("Config changed: tried preparing the service but returned False")
                raise DPBenchmarkError()

            # We must set the file, as we were not UNSET:
            if status in [DPBenchmarkExecStatus.RUNNING, DPBenchmarkExecStatus.ERROR]:
                self.run()
        except DPBenchmarkError:
            event.defer()

        self._set_status()

    def _on_relation_broken(self, event: EventBase) -> None:
        self.stop()
        self.clean_up()

    def scrape_config(self) -> List[Dict]:
        """Generate scrape config for the Patroni metrics endpoint."""
        return [
            {
                "metrics_path": "/metrics",
                "static_configs": [{"targets": [f"{self._unit_ip()}:{METRICS_PORT}"]}],
                "tls_config": {"insecure_skip_verify": True},
                "scheme": "http",
            }
        ]

    def _install_packages(self, extra_packages: List[str] = []) -> None:
        """Install the packages needed for the benchmark."""
        self.unit.status = MaintenanceStatus("Installing apt packages...")
        apt.update()
        apt.add_package(extra_packages)
        self.unit.status = ActiveStatus()

    def _on_install(self, event: EventBase) -> None:
        """Installs the basic packages and python dependencies.

        No exceptions are captured as we need all the dependencies below to even start running.
        """
        self.unit.status = MaintenanceStatus("Installing...")
        self.service.render_service_executable()
        self.unit.status = ActiveStatus()

    def _on_peer_changed(self, event: EventBase) -> None:
        """Peer relation changed."""
        if (
            not self.unit.is_leader()
            and self.benchmark_status.app_status() == DPBenchmarkExecStatus.PREPARED
            and self.benchmark_status.service_status()
            not in [DPBenchmarkExecStatus.PREPARED, DPBenchmarkExecStatus.RUNNING]
        ):
            # We need to mark this unit as prepared so we can rerun the script later
            self.benchmark_status.set(DPBenchmarkExecStatus.PREPARED)

    def check(self) -> DPBenchmarkExecStatus | None:
        """Wraps the status check and catches the wrong state error for processing."""
        try:
            return self.benchmark_status.check()
        except DPBenchmarkIsInWrongStateError:
            logger.warning("check: Benchmark is in the wrong state")
        return None

    def on_list_workloads_action(self, event: EventBase) -> None:
        """Lists all possible workloads."""
        event.set_results({"workloads": self.supported_workloads()})

    def on_prepare_action(self, event: EventBase) -> None:
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

        self.unit.status = MaintenanceStatus("Running prepare command...")
        try:
            self.prepare()
        except DPBenchmarkMultipleRelationsToDBError:
            event.fail("Failed: missing database options")
        except DPBenchmarkExecError:
            event.fail("Failed: error in benchmark while executing prepare")
        except DPBenchmarkStatusError:
            event.fail("Failed: missing database options")
        else:
            event.set_results({"status": "prepared"})
        self._set_status()

    def prepare(self) -> None:
        """Prepares the database and sets the state.

        Raises:
            DPBenchmarkMultipleRelationsToDBError: If there are multiple relations to the database.
            DPBenchmarkExecError: If the benchmark execution fails.
            DPBenchmarkStatusError: If the benchmark is not in the correct status.
        """
        if self.unit.is_leader():
            self.service.exec("prepare", self.labels)

        if not self.service.prepare(
            db=self.database.get_execution_options(),
            workload_name=self.config["workload_name"],
            labels=self.labels,
            extra_config=str(self.database.get_execution_options().extra),
        ):
            raise DPBenchmarkStatusError(DPBenchmarkExecStatus.ERROR)
        self.benchmark_status.set(DPBenchmarkExecStatus.PREPARED)

    def on_run_action(self, event: EventBase) -> None:
        """Run benchmark action."""
        try:
            self.run()
            event.set_results({"status": "running"})
        except DPBenchmarkUnitNotReadyError:
            event.fail(
                f"Failed: app level reports {self.benchmark_status.app_status()} and service level reports {self.benchmark_status.service_status()}"
            )
        except DPBenchmarkStatusError as e:
            event.fail(f"Failed: benchmark must not be in status {e.status.value}")
            return
        except DPBenchmarkServiceError as e:
            event.fail(f"Failed: error in benchmark service {e}")
            return
        self._set_status()

    def run(self) -> None:
        """Run the benchmark service.

        Raises:
            DPBenchmarkServiceError: Returns an error if the service fails to start.
            DPBenchmarkStatusError: Returns an error if the benchmark is not in the correct status.
            DPEBenchmarkUnitNotReadyError: If the benchmark unit is not ready.
        """
        if not (status := self.check()):
            raise DPBenchmarkUnitNotReadyError()
        if status == DPBenchmarkExecStatus.ERROR:
            logger.debug("Overriding ERROR status and restarting service")
        elif status not in [
            DPBenchmarkExecStatus.PREPARED,
            DPBenchmarkExecStatus.STOPPED,
        ]:
            raise DPBenchmarkStatusError(status)
        self.service.run()
        self.benchmark_status.set(DPBenchmarkExecStatus.RUNNING)

    def on_stop_action(self, event: EventBase) -> None:
        """Stop benchmark service."""
        if not self.check():
            event.fail(
                f"Failed: app level reports {self.benchmark_status.app_status()} and service level reports {self.benchmark_status.service_status()}"
            )
            return

        try:
            self.stop()
            event.set_results({"status": "stopped"})
        except (DPBenchmarkUnitNotReadyError, DPBenchmarkServiceError):
            event.fail(
                f"Failed: app level reports {self.benchmark_status.app_status()} and service level reports {self.benchmark_status.service_status()}"
            )
            return
        self._set_status()

    def stop(self) -> None:
        """Stop the benchmark service. Returns true if stop was successful.

        Raises:
            DPBenchmarkUnitNotReadyError: If the benchmark unit is not ready.
            DPBenchmarkServiceError: Returns an error if the service fails to stop.
        """
        if not (status := self.check()):
            raise DPBenchmarkUnitNotReadyError()

        if status in [DPBenchmarkExecStatus.RUNNING, DPBenchmarkExecStatus.ERROR]:
            logger.debug("The benchmark is running, stopped or in error, stop it")
            self.service.stop()
        else:
            logger.debug("Service is already stopped.")

        if self.model.relations.get(PEER_RELATION):
            # There are some situations where we may be going away and then we will not see the relation databag anymore.
            self.benchmark_status.set(DPBenchmarkExecStatus.STOPPED)

    def on_clean_action(self, event: EventBase) -> None:
        """Clean the database."""
        try:
            if not self.clean_up():
                event.fail("Failed: error in benchmark while executing clean")
                return
        except DPBenchmarkUnitNotReadyError:
            event.fail(
                f"Failed: app level reports {self.benchmark_status.app_status()} and service level reports {self.benchmark_status.service_status()}"
            )
        except DPBenchmarkMissingOptionsError:
            event.fail("Failed: missing database options")
            return
        except DPBenchmarkExecError:
            event.fail("Failed: error in benchmark while executing clean")
            return
        except DPBenchmarkServiceError as e:
            event.fail(f"Failed: error in benchmark service {e}")
            return
        self._set_status()

    def clean_up(self) -> bool:
        """Clean up the database and the unit.

        We recheck the service status, as we do notw ant to make any distinctions between the different steps.

        Raises:
            DPBenchmarkUnitNotReadyError: If the benchmark unit is not ready.
            DPEBenchmarkMissingOptionsError: If the benchmark options are missing at self.service.exec
            DPBenchmarkExecError: If the benchmark execution fails at self.service.exec.
            DPBenchmarkServiceError: service related failures
        """
        if not (status := self.check()):
            raise DPBenchmarkUnitNotReadyError()

        svc = self.service
        if status == DPBenchmarkExecStatus.UNSET:
            logger.debug("benchmark units are idle, but continuing anyways")
        if status in [DPBenchmarkExecStatus.RUNNING, DPBenchmarkExecStatus.ERROR]:
            logger.info("benchmark service stopped in clean action")
            svc.stop()

        if self.unit.is_leader():
            self.service.exec("clean", self.labels)
        svc.unset()
        self.benchmark_status.set(DPBenchmarkExecStatus.UNSET)
        return True

    def supported_workloads(self) -> list[str]:
        """List the supported workloads."""
        return [
            ".".join(name.split(".")[:-2])
            for name in os.listdir("src/workload_parameter_templates")
        ]
