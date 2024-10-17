#!/usr/bin/python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""This method runs the opensearch-benchmark call, collects its output and forwards to prometheus."""

import argparse
import os
import signal
import subprocess
import sys
import time
import uuid

from prometheus_client import Gauge, start_http_server


class OSBService:
    """OpenSearch Benchmark service class."""

    def __init__(
        self,
        target_hosts: list[str],
        workload: str,
        db_user: str,
        db_password: str,
        *,
        kill_running_processes: bool = True,
        duration: int = 0,
        workload_params: str|None = None,
        test_mode: bool = False,
    ):
        self.results_file = f"/tmp/osb_results_{int(time.time())}_{str(uuid.uuid4())[6:]}.csv"

        self.osb_args = f""" --workload {workload}
         --target-hosts {','.join(target_hosts)}
         --pipeline benchmark-only
         --client-options basic_auth_user:{db_user},basic_auth_password:{db_password},verify_certs:false,max_connections:2000,timeout:600
         --results-format csv --results-file {self.results_file}
         --show-in-results all"""

        if kill_running_processes:
            self.osb_args += " --kill-running-processes"

        if workload_params:
            self.osb_args += f" --workload-params {workload_params}"

        if test_mode:
            self.osb_args += " --test-mode"

        self.duration = duration

    def cmd(self, runtype):
        return f"""/usr/local/bin/opensearch-benchmark {runtype} """ + self.osb_args

    def _exec(self, runtype):
        subprocess.check_output(self.cmd(runtype).split(), timeout=86400)

    def start(self):
        """Start the OSB benchmark."""
        self._exec("execute-test")

    def _process_line(self, line):
        # Processes line in format: Metric,Task,Value,Unit
        if len(line.split(",")) != 4:
            return None
        return {
            "title": line.split(",")[0],
            "task": line.split(",")[1],
            "value": line.split(",")[2],
            "unit": line.split(",")[3],
        }

    def wait_and_process(self, proc, metrics, label, extra_labels):
        """Run one step of the main benchmark service loop."""
        for line in iter(proc.stdout.readline, ""):
            # Dummy line reader
            # OpenSearch Benchmark prints status of its execution only
            # We are more interested to know when is it finished to run
            #
            #  print(f"STDOUT: {line}")
            time.sleep(5)
            if proc.poll() is not None:
                # Process has finished
                break

        # Open the files with results and start uploading that to prometheus.
        with open(self.results_file) as f:
            # First line is a header, ignore
            f.readline()

            for line in f:
                value = self._process_line(line.rstrip())
                if not value or not value["value"]:
                    continue
                add_benchmark_metric(
                    metrics, f"{label}_{value['title'].replace(' ', '_').lower()}", extra_labels, f"{value['title']} ({value['unit']})", value["value"]
                )

    def stop(self, proc):
        """Stop the service with SIGTERM."""
        proc.terminate()

    def clean(self):
        """Clean the benchmark database."""
        pass


def add_benchmark_metric(metrics, label, extra_labels, description, value):
    """Add the benchmark to the prometheus metric."""
    if label not in metrics:
        metrics[label] = Gauge(label, description, ["model", "unit"])
    metrics[label].labels(*extra_labels).set(value)


keep_running = True


def main(args):
    """Run main method."""
    global keep_running

    def _exit(*args, **kwargs):
        global keep_running
        keep_running = False  # noqa: F841

    signal.signal(signal.SIGINT, _exit)
    signal.signal(signal.SIGTERM, _exit)
    start_http_server(8088)

    if args.command == "run":
        initial_time = int(time.time())
        finish_time = initial_time + args.duration
        metrics = {}
        while keep_running:
            svc = OSBService(
                target_hosts=args.target_hosts.split(","),
                workload=args.workload,
                db_user=args.db_user,
                db_password=args.db_password,
                kill_running_processes=True,
                duration=args.duration,
                workload_params=args.workload_params,
                test_mode=args.test_mode,
            )

            proc = subprocess.Popen(
                svc.cmd("execute-test").split(),
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
            )

            svc.wait_and_process(proc, metrics, "osb", [] if not args.extra_labels else args.extra_labels.split(","))

            if (
                # Either we have reached a timeout
                (int(time.time()) >= finish_time and args.duration != 0)

                # Or the process has finished and --duration != 0 (so, do not keep rerunning)
                or (args.duration != 0 and proc.poll() is None)
            ):
                # Finish the processing
                keep_running = False

        print(f"benchmark STDOUT: {proc.stdout.read()}")
        if not keep_running:
            # It means we have requested the main process to finish
            # Now, check if we also need to terminate current benchmark
            if proc.poll() is None:
                # We have received a keep_running=False but still running. Terminate it
                # This will end the process with -15, which is SIGTERM
                svc.stop(proc)
            sys.exit(0)
        if proc.poll() != 0:
            # Make sure we report a failure to systemd
            print(f"benchmark STDERR: {proc.stderr.read()}")
            raise Exception(f"benchmark failed with {proc.poll()}")
    elif args.command == "clean":
        svc = OSBService(
            target_hosts=args.target_hosts.split(","),
            workload=args.workload,
            db_user=args.db_user,
            db_password=args.db_password,
            kill_running_processes=True,
            duration=args.duration,
            workload_params=args.workload_params,
            test_mode=args.test_mode,
        )
        svc.clean()
    else:
        raise Exception(f"Command option {args.command} not known")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="osb_svc", description="Runs the benchmark command as an argument."
    )
    parser.add_argument("--command", type=str, help="Command to be executed", default="run")
    parser.add_argument("--target_hosts", type=str, help="comma-separated list of target hosts")
    parser.add_argument("--workload", type=str, help="Name of the workload to be executed")
    parser.add_argument("--threads", type=int, default=1)
    parser.add_argument("--clients", type=int, default=16)
    parser.add_argument("--db_user", type=str)
    parser.add_argument("--db_password", type=str)
    parser.add_argument("--duration", type=int, default=0)
    parser.add_argument("--workload_params", type=str, default=None)
    parser.add_argument(
        "--extra_labels", type=str, help="comma-separated list of extra labels to be used.", default=""
    )
    parser.add_argument(
        "--test_mode", action='store_true', help="Whether to run in test mode",
    )

    args = parser.parse_args()

    main(args)
