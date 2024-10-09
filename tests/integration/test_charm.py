#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

import asyncio
import logging
import subprocess
from types import SimpleNamespace

import pytest
from pytest_operator.plugin import OpsTest
from tenacity import Retrying, stop_after_delay, wait_fixed

from .helpers import (
    ALL_GROUPS,
    APP_NAME,
    DURATION,
    MODEL_CONFIG,
    SERIES,
    SMALL_DEPLOYMENTS,
    TLS_CERTIFICATES_APP_NAME,
)

logger = logging.getLogger(__name__)


model_db = None


def check_service(svc_name: str, retry_if_fail: bool = True):
    if not retry_if_fail:
        return subprocess.check_output(
            ["juju", "ssh", f"{APP_NAME}/0", "--", "sudo", "systemctl", "is-active", svc_name],
            text=True,
        )
    for attempt in Retrying(stop=stop_after_delay(150), wait=wait_fixed(15)):
        with attempt:
            return subprocess.check_output(
                ["juju", "ssh", f"{APP_NAME}/0", "--", "sudo", "systemctl", "is-active", svc_name],
                text=True,
            )


async def run_action(
    ops_test, action_name: str, unit_name: str, timeout: int = 30, **action_kwargs
):
    """Runs the given action on the given unit."""
    client_unit = ops_test.model.units.get(unit_name)
    action = await client_unit.run_action(action_name, **action_kwargs)
    result = await action.wait()
    logging.info(f"request results: {result.results}")
    return SimpleNamespace(status=result.status or "completed", response=result.results)


@pytest.mark.parametrize("deploy_type", SMALL_DEPLOYMENTS)
@pytest.mark.abort_on_fail
@pytest.mark.skip_if_deployed
async def test_build_and_deploy_small_deployment(ops_test: OpsTest, deploy_type: str) -> None:
    """Build and deploy an OpenSearch cluster."""
    my_charm = await ops_test.build_charm(".")

    model_conf = MODEL_CONFIG.copy()
    # Make it more regular as COS relation-broken really happens on the
    # next hook call in each opensearch unit.
    # If this value is changed, then update the sleep accordingly at:
    #  test_prometheus_exporter_disabled_by_cos_relation_gone
    model_conf["update-status-hook-interval"] = "1m"
    await ops_test.model.set_config(model_conf)

    # Deploy TLS Certificates operator.
    config = {"ca-common-name": "CN_CA"}
    await asyncio.gather(
        ops_test.model.deploy(
            my_charm,
            num_units=1,
            series=SERIES,
        ),
        ops_test.model.deploy("opensearch", num_units=3, series=SERIES, channel="2/edge"),
        ops_test.model.deploy(TLS_CERTIFICATES_APP_NAME, channel="stable", config=config),
    )

    # Relate it to OpenSearch to set up TLS.
    await ops_test.model.integrate("opensearch", TLS_CERTIFICATES_APP_NAME)
    await ops_test.model.integrate("opensearch", APP_NAME)
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME], status="active", timeout=1000, idle_period=120
    )

    assert len(ops_test.model.applications[APP_NAME].units) == 3


# @pytest.mark.parametrize("db_driver,use_router", ALL_GROUPS)
# @pytest.mark.abort_on_fail
# async def test_run_action_and_cause_failure(ops_test: OpsTest, db_driver, use_router) -> None:
#     """Starts a run and then kills the sysbench process. Systemd must then report it as failed."""
#     app = ops_test.model.applications[APP_NAME]
#     await app.set_config({"duration": "0"})

#     output = await run_action(ops_test, "run", f"{APP_NAME}/0")
#     assert output.status == "completed"

#     svc = "sysbench.service"

#     # Make sure we are currently running
#     assert "inactive" not in check_service(svc)
#     # Now, figure out sysbench's PID itself
#     # The check_output is getting strange chars in the output as we, filter it out
#     sysbench_svc_pid = re.findall(
#         r"MainPID=[0-9.]+",
#         subprocess.check_output(
#             f"juju ssh {APP_NAME}/0 -- systemctl show --property=MainPID {svc}",
#             text=True,
#             shell=True,
#         ),
#     )[0].split("MainPID=")[1]
#     pid = (
#         subprocess.check_output(
#             [
#                 "juju",
#                 "ssh",
#                 f"{APP_NAME}/0",
#                 "--",
#                 "sudo",
#                 "cat",
#                 f"/proc/{sysbench_svc_pid}/task/{sysbench_svc_pid}/children",
#             ],
#             text=True,
#         )
#         .split("\n")[0]
#         .split(" ")[0]
#     )
#     # Now, kill the sysbench process
#     subprocess.check_output(["juju", "ssh", f"{APP_NAME}/0", "--", "sudo", "kill", "-9", str(pid)])

#     # Finally, check if the service is now in a failed state in systemd
#     try:
#         subprocess.check_output([
#             "juju",
#             "ssh",
#             f"{APP_NAME}/0",
#             "--",
#             "sudo",
#             "systemctl",
#             "is-failed",
#             svc,
#         ])
#     except subprocess.CalledProcessError as e:
#         # We expect "is-failed" to succeed, i.e. we have a failed service
#         raise AssertionError(f"Service {svc} is not in a failed state") from e

#     async with ops_test.fast_forward("60s"):
#         # Check if the charm is now blocked:
#         await ops_test.model.wait_for_idle(
#             apps=[APP_NAME],
#             status="blocked",
#             timeout=15 * 60,
#         )


@pytest.mark.parametrize("deploy_type", ALL_GROUPS)
@pytest.mark.abort_on_fail
async def test_run_action(ops_test: OpsTest, deploy_type) -> None:
    """Try to run the benchmark for DURATION and then wait until it is finished."""
    app = ops_test.model.applications[APP_NAME]
    await app.set_config({"duration": f"{DURATION}"})

    output = await run_action(ops_test, "run", f"{APP_NAME}/0")
    assert output.status == "completed"

    svc_output = check_service("sysbench.service")
    logger.info(f"sysbench.service output: {svc_output}")

    # Looks silly, but we "active" is in "inactive" string :(
    assert "inactive" not in svc_output and "active" in svc_output

    async with ops_test.fast_forward("60s"):
        # Wait until it is finished, and retry
        await asyncio.sleep(3 * DURATION)

        await ops_test.model.wait_for_idle(
            apps=[APP_NAME],
            status="blocked",
            timeout=15 * 60,
        )

        try:
            logger.info("Checking if sysbench.service is inactive")
            svc_output = check_service("sysbench.service", retry_if_fail=False)
        except subprocess.CalledProcessError:
            # Finished running and check_output for "systemctl is-active" will fail
            return
        # Did not fail, so check if we got a "inactive" in the output
        assert "inactive" in svc_output


@pytest.mark.parametrize("deploy_type", ALL_GROUPS)
@pytest.mark.abort_on_fail
async def test_clean_action(ops_test: OpsTest, deploy_type) -> None:
    """Validate clean action."""
    output = await run_action(ops_test, "clean", f"{APP_NAME}/0")
    assert output.status == "completed"

    await ops_test.model.wait_for_idle(
        apps=[APP_NAME],
        status="active",
        raise_on_blocked=True,
        timeout=15 * 60,
    )

    for svc_name in ["sysbench.service", "sysbench_prepared.target"]:
        try:
            svc_output = check_service(svc_name, retry_if_fail=False)
        except subprocess.CalledProcessError:
            # Finished running and check_output for "systemctl is-active" will fail
            pass
        else:
            assert "inactive" in svc_output
