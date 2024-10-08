#!/usr/bin/python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""This method runs the opensearch-benchmark call, collects its output and forwards to prometheus."""

import sys
import os
import argparse
import signal
import subprocess
import time

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
        workload_params: str|None = "./osb_benchmark.json",
    ):
        self.results_file = f"/tmp/osb_results_{int(time.time())}.csv"

        self.osb_args = f"""--target-hosts {','.join(target_hosts)}
         --pipeline benchmark-only
         --workload-params {workload_params}
         --client-options basic_auth_user:{db_user},basic_auth_password:{db_password},verify_certs:false,max_connections:2000,timeout:600
         --results-format csv --results-file {self.results_file}
         --show-in-results all"""

        if kill_running_processes:
            self.osb_args += " --kill-running-processes"

        if workload_params:
            self.osb_args += f"--workload {workload}"
        self.duration = duration

    def _exec(self, cmd):
        subprocess.check_output(f"""/usr/bin/opensearch-benchmark {cmd}""" + self.osb_args, timeout=self.duration)

    def start(self):
        """Start the OSB benchmark."""
        self._exec("execute-test")

    def prepare(self):
        """Prepare the OSB output."""
        pass

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

    def run(self, proc, metrics, label, extra_labels):
        """Run one step of the main benchmark service loop."""
        for _ in iter(proc.stdout.readline, ""):
            # Dummy line reader
            # OpenSearch Benchmark prints status of its execution only
            # We are more interested to know when is it finished to run
            time.sleep(5)

        # We are now finished.
        # Open the files with results and start uploading that to prometheus.
        with open(self.results_file) as f:
            for line in iter(f.readline()):
                value = self._process_line(line)
                if not value:
                    continue
                add_benchmark_metric(
                    metrics, f"{label}_{value['title'].replace('_').lower()}", extra_labels, f"{value['title']} (value['unit'])", value["unit"]
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

    svc = OSBService(
        target_hosts=args.target_hosts,
        workload=args.workload,
        db_user=args.db_user,
        db_password=args.db_password,
        results_file=args.results_file,
        kill_running_processes=True,
        duration=args.duration,
        workload_params=args.workload_params,
    )

    signal.signal(signal.SIGINT, _exit)
    signal.signal(signal.SIGTERM, _exit)
    start_http_server(8088)

    if args.command == "prepare":
        svc.prepare()
        keep_running = False  # Gracefully shutdown
    elif args.command == "run":
        proc = subprocess.Popen(
            svc.sysbench.start(),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
        )
        metrics = {}

        initial_time = int(time.time())
        finish_time = initial_time + args.duration

        while keep_running and proc.poll() is None:
            svc.run(proc, metrics, f"osb", args.extra_labels.split(","))
            if int(time.time()) >= finish_time:
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
    parser.add_argument("--workload_params", type=str, default="./osb_benchmark.json")
    parser.add_argument(
        "--extra_labels", type=str, help="comma-separated list of extra labels to be used."
    )

    args = parser.parse_args()

    main(args)
