"""Opt-in, read-only trainability counting for a configurable job."""

import os

import pytest

from wt_sdk import WTGatewayClient
from wt_sdk.etl.tools.trainability_sample import sample_job


def test_count_sampled_job_trainability():
    job_id = os.getenv("TRAINABILITY_TEST_JOB_ID")
    if not job_id:
        pytest.skip("set TRAINABILITY_TEST_JOB_ID to count trainable sessions")
    session_count = int(os.getenv("TRAINABILITY_TEST_SESSION_COUNT", "10"))
    seed = int(os.getenv("TRAINABILITY_TEST_SEED", "0"))
    with WTGatewayClient() as client:
        report = sample_job(
            client,
            job_id=job_id,
            session_count=session_count,
            seed=seed,
            table=client.config.tables.landing_table,
        )
    print(f"DEBUG job_id={job_id}")
    print(f"DEBUG sessions_processed={report.get('sessions_processed')}")
    print(f"DEBUG sessions_failed={report.get('sessions_failed')}")
    print(f"DEBUG trainable_row_count={report.get('trainable_row_count')}")
    for session in report.get("sessions", []):
        print(
            f"DEBUG session={session['session_id']} "
            f"status={session['status']} rows={session.get('row_count')} "
            f"eligible={session.get('eligible_row_count')} "
            f"trainable={session.get('is_trainable_true_count')} "
            f"error_type={session.get('error_type')} error={session.get('error')}"
        )
    trainability_annotation_count = sum(
        1
        for session in report["sessions"]
        if session["status"] == "processed"
        for row in session["rows"]
        if row["is_trainable"] is True
    )
    print(f"Trainability annotations: {trainability_annotation_count}")
    assert report["sessions_failed"] == 0
    assert report["sessions_processed"] == session_count
    assert report["trainable_row_count"] == trainability_annotation_count
