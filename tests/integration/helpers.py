# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
from pathlib import Path

import pytest
import yaml

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
DURATION = 10
TLS_CERTIFICATES_APP_NAME = "self-signed-certificates"
SERIES = "jammy"
UNIT_IDS = [0, 1, 2]
IDLE_PERIOD = 75


logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)s", datefmt="%H:%M:%S"
)
logger = logging.getLogger(__name__)


MODEL_CONFIG = {
    "logging-config": "<root>=INFO;unit=DEBUG",
    "update-status-hook-interval": "5m",
    "cloudinit-userdata": """postruncmd:
        - [ 'sysctl', '-w', 'vm.max_map_count=262144' ]
        - [ 'sysctl', '-w', 'fs.file-max=1048576' ]
        - [ 'sysctl', '-w', 'vm.swappiness=0' ]
        - [ 'sysctl', '-w', 'net.ipv4.tcp_retries2=5' ]
    """,
}


ALL_GROUPS = {
    deploy_type: pytest.param(
        deploy_type,
        id=deploy_type,
        marks=[
            pytest.mark.group(deploy_type),
            pytest.mark.runner([
                "self-hosted",
                "linux",
                "X64",
                "jammy",
                "xlarge" if deploy_type == "large" else "large",
            ]),
        ],
    )
    for deploy_type in ["large_deployment", "small_deployment"]
}

ALL_DEPLOYMENTS = list(ALL_GROUPS.values())
SMALL_DEPLOYMENTS = [ALL_GROUPS["small_deployment"]]
LARGE_DEPLOYMENTS = [ALL_GROUPS["large_deployment"]]
