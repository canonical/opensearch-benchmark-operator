# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""This module describes the basic benchmark interface."""

from abc import ABC, abstractmethod


class DPBenchmarkCharmInterface(ABC):
    """Each benchmark charm must be able to implement the methods below.

    This class allows to separate the actual tasks and service handling from the charm itself.
    Methods related to actions or relation events will call the methods below.
    """

    @abstractmethod
    def list_supported_workloads(self) -> list[str]:
        """List the supported workloads."""
        raise NotImplementedError

    @abstractmethod
    def execute_benchmark_cmd(self, extra_labels, command: str):
        """Execute the benchmark command."""
        raise NotImplementedError

    @abstractmethod
    def setup_db_relation(self, relation_names: list[str]):
        """Setup the database relation."""
        raise NotImplementedError

    @abstractmethod
    def prepare(self):
        """Prepares the database and sets the state.

        Raises:
            DPBenchmarkMultipleRelationsToDBError: If there are multiple relations to the database.
            DPBenchmarkExecError: If the benchmark execution fails.
        """
        raise NotImplementedError

    @abstractmethod
    def run(self):
        """Run the benchmark service.

        Raises:
            DPBenchmarkServiceError: Returns an error if the service fails to start.
            DPBenchmarkStatusError: Returns an error if the benchmark is not in the correct status.
            DPEBenchmarkUnitNotReadyError: If the benchmark unit is not ready.
        """
        raise NotImplementedError

    @abstractmethod
    def stop(self):
        """Stop the benchmark service.

        Raises:
            DPBenchmarkServiceError: Returns an error if the service fails to stop.
        """
        raise NotImplementedError

    @abstractmethod
    def clean_up(self):
        """Clean up the database and the unit.

        We recheck the service status, as we do notw ant to make any distinctions between the different steps.

        Raises:
            DPBenchmarkUnitNotReadyError: If the benchmark unit is not ready.
            DPEBenchmarkMissingOptionsError: If the benchmark options are missing at execute_benchmark_cmd
            DPBenchmarkExecError: If the benchmark execution fails at execute_benchmark_cmd.
            DPBenchmarkServiceError: service related failures
        """
        raise NotImplementedError
