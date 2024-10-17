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
from typing import Dict, List

import ops
from charms.grafana_agent.v0.cos_agent import COSAgentProvider
from charms.operator_libs_linux.v0 import apt
from overrides import override

from benchmark.abs_benchmark_charm import DPBenchmarkCharmInterface
from benchmark.constants import (
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
from benchmark.relation_manager import DatabaseRelationManager
from benchmark.service import DPBenchmarkService
from benchmark.status import BenchmarkStatus

# Log messages can be retrieved using juju debug-log
logger = logging.getLogger(__name__)


class DPBenchmarkCharm(ops.CharmBase, DPBenchmarkCharmInterface):
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
        self.benchmark_status = BenchmarkStatus(self, PEER_RELATION, self.SERVICE_CLS())
        self.labels = ",".join([self.model.name, self.unit.name])

        # We need to narrow the options of workload_name to the supported ones
        if self.config.get("workload_name", "nyc_taxis") not in self.list_supported_workloads():
            self.unit.status = ops.model.BlockedStatus("Unsupported workload")
            logger.error(f"Unsupported workload {self.config.get('workload_name', 'nyc_taxis')}")
            # Assert exception makes sense here
            # Let the unit error out
            assert (
                self.config.get("workload_name", "nyc_taxis") in self.list_supported_workloads()
            ), "Invalid workload name"

    @override
    def setup_db_relation(self, relation_names: List[str]):
        """Setup the database relation."""
        self.database = DatabaseRelationManager(self, relation_names)
        self.framework.observe(self.database.on.db_config_update, self._on_config_changed)

    def _on_update_status(self, _):
        """Update the status of the charm."""
        self._set_status()

    def _set_status(self) -> None:
        """Recovers the benchmark status."""
        status = self.benchmark_status.check()
        if not status:
            self.unit.status = ops.model.WaitingStatus("Benchmark not ready")
            return

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

    def _on_config_changed(self, event):
        """Config changed event."""
        try:
            # First, we check if the status of the service
            if (
                not (status := self.benchmark_status.check())
                or status == DPBenchmarkExecStatus.UNSET
            ):
                logger.debug(
                    "The config changed happened too early in the lifecycle, nothing to do"
                )
                return

            if status in [DPBenchmarkExecStatus.RUNNING, DPBenchmarkExecStatus.ERROR]:
                logger.debug("The benchmark is running, stopped or in error, stop it")
                self.stop()

            self.clean_up()
            self.prepare()

            # We must set the file, as we were not UNSET:
            if status in [DPBenchmarkExecStatus.RUNNING, DPBenchmarkExecStatus.ERROR]:
                self.run()
        except DPBenchmarkError as e:
            logger.error(f"Error in config changed: {e}")
            event.defer()
            return

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

    def on_list_workloads_action(self, event):
        """Lists all possible workloads."""
        event.set_results({"workloads": self.list_supported_workloads()})

    def on_prepare_action(self, event):
        """Prepare the database.

        There are two steps: the actual prepare command and setting a target to inform the
        prepare was successful.
        """
        self.unit.status = ops.model.MaintenanceStatus("Checking status...")
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
            self.prepare()
        except DPBenchmarkMultipleRelationsToDBError:
            event.fail("Failed: missing database options")
            return
        except DPBenchmarkExecError:
            event.fail("Failed: error in benchmark while executing prepare")
            return
        except DPBenchmarkStatusError:
            event.fail("Failed: missing database options")
            return

        event.set_results({"status": "prepared"})

    def prepare(self):
        """Prepares the database and sets the state.

        Raises:
            DPBenchmarkMultipleRelationsToDBError: If there are multiple relations to the database.
            DPBenchmarkExecError: If the benchmark execution fails.
        """
        if self.unit.is_leader():
            self.execute_benchmark_cmd(self.labels, "prepare")

        if not self.SERVICE_CLS().prepare(
            db=self.database.get_execution_options(),
            workload_name=self.config["workload_name"],
            labels=self.labels,
            extra_config=str(self.database.get_execution_options().extra),
        ):
            raise DPBenchmarkStatusError(DPBenchmarkExecStatus.ERROR)
        self.benchmark_status.set(DPBenchmarkExecStatus.PREPARED)

    def on_run_action(self, event):
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
        except DPBenchmarkServiceError as e:
            event.fail(f"Failed: error in benchmark service {e}")

    def run(self):
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

        svc = self.SERVICE_CLS()
        svc.run()
        self.benchmark_status.set(DPBenchmarkExecStatus.RUNNING)

    def on_stop_action(self, event):
        """Stop benchmark service."""
        if not (status := self.check()):
            event.fail(
                f"Failed: app level reports {self.benchmark_status.app_status()} and service level reports {self.benchmark_status.service_status()}"
            )
            return
        if status not in [DPBenchmarkExecStatus.RUNNING, DPBenchmarkExecStatus.ERROR]:
            event.set_results({"status": "benchmark was not running"})
            return
        self.stop()
        event.set_results({"status": "stopped"})

    def stop(self):
        """Stop the benchmark service.

        Raises:
            DPBenchmarkServiceError: Returns an error if the service fails to stop.
        """
        svc = self.SERVICE_CLS()
        svc.stop()
        self.benchmark_status.set(DPBenchmarkExecStatus.STOPPED)

    def on_clean_action(self, event):
        """Clean the database."""
        try:
            self.clean_up()
        except DPBenchmarkUnitNotReadyError:
            event.fail(
                f"Failed: app level reports {self.benchmark_status.app_status()} and service level reports {self.benchmark_status.service_status()}"
            )
        except DPBenchmarkMissingOptionsError:
            event.fail("Failed: missing database options")
        except DPBenchmarkExecError:
            event.fail("Failed: error in benchmark while executing clean")
        except DPBenchmarkServiceError as e:
            event.fail(f"Failed: error in benchmark service {e}")

    def clean_up(self):
        """Clean up the database and the unit.

        We recheck the service status, as we do notw ant to make any distinctions between the different steps.

        Raises:
            DPBenchmarkUnitNotReadyError: If the benchmark unit is not ready.
            DPEBenchmarkMissingOptionsError: If the benchmark options are missing at execute_benchmark_cmd
            DPBenchmarkExecError: If the benchmark execution fails at execute_benchmark_cmd.
            DPBenchmarkServiceError: service related failures
        """
        if not (status := self.check()):
            raise DPBenchmarkUnitNotReadyError()

        svc = self.SERVICE_CLS()
        if status == DPBenchmarkExecStatus.UNSET:
            logger.debug("benchmark units are idle, but continuing anyways")
        if status == DPBenchmarkExecStatus.RUNNING:
            logger.info("benchmark service stopped in clean action")
            svc.stop()

        self.unit.status = ops.model.MaintenanceStatus("Cleaning up database")
        if self.unit.is_leader():
            self.execute_benchmark_cmd(self.labels, "clean")
        svc.unset()
        self.benchmark_status.set(DPBenchmarkExecStatus.UNSET)
