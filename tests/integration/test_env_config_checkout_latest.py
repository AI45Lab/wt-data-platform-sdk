import time
import uuid

from wt_sdk import EnvConfigManager


def test_env_config_reads_checkout_latest_by_default_across_managers():
    suffix = uuid.uuid4().hex
    env_id = f"wt-sdk-integration-env-{suffix}"
    job_id = f"wt-sdk-integration-job-{suffix}"

    reader = EnvConfigManager(profile="test")
    writer = EnvConfigManager(profile="test")
    try:
        writer.save_config(
            {
                "env_name": "wt-sdk-integration-env",
                "env_id": env_id,
                "job_id": job_id,
                "group_id": "wt-sdk-integration-group",
                "finished": False,
                "env_params": {"source": "checkout_latest_integration"},
                "image": "wt-sdk-integration-image",
                "created_at": int(time.time()),
            }
        )

        rows = reader.get_env_configs(
            limit=10,
            offset=0,
            filter_query=f"env_id = '{env_id}'",
        )

        assert len(rows) == 1
        assert rows[0]["env_id"] == env_id
        assert rows[0]["job_id"] == job_id
        assert rows[0]["env_params"] == {"source": "checkout_latest_integration"}
    finally:
        try:
            writer.delete_config(env_id)
        finally:
            reader.close()
            writer.close()
