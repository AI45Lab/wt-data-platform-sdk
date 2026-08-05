"""Keep ETL network integration tests explicitly opt-in."""

import os

import pytest


def pytest_collection_modifyitems(config, items):
    del config
    run_integration = os.getenv("WT_SDK_RUN_INTEGRATION") == "1"
    for item in items:
        if "/integration/" not in str(item.fspath):
            continue
        item.add_marker(pytest.mark.integration)
        if not run_integration:
            item.add_marker(
                pytest.mark.skip(
                    reason="set WT_SDK_RUN_INTEGRATION=1 to run DLDB/S3 integration tests",
                )
            )
