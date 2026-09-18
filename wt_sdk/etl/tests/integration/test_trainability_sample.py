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
    session_offset_value = os.getenv("TRAINABILITY_TEST_SESSION_OFFSET")
    session_offset = (
        int(session_offset_value) if session_offset_value is not None else None
    )
    summary_only = os.getenv("TRAINABILITY_TEST_SUMMARY_ONLY", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    with WTGatewayClient() as client:
        report = sample_job(
            client,
            job_id=job_id,
            session_count=session_count,
            seed=seed,
            table=client.config.tables.landing_table,
            session_offset=session_offset,
            include_rows=not summary_only,
        )
    print(f"DEBUG job_id={job_id}")
    print(f"DEBUG available_sessions={report['sampling']['available_sessions']}")
    print(f"DEBUG session_offset={report['sampling']['session_offset']}")
    print(f"DEBUG sessions_processed={report.get('sessions_processed')}")
    print(f"DEBUG sessions_failed={report.get('sessions_failed')}")
    print(f"DEBUG trainable_row_count={report.get('trainable_row_count')}")
    for session in report.get("sessions", []):
        print(
            f"DEBUG session={session['session_id']} "
            f"status={session['status']} rows={session.get('row_count')} "
            f"eligible={session.get('eligible_row_count')} "
            f"trainable={session.get('is_trainable_true_count')} "
            f"trainable_steps={session.get('trainable_step_ids')} "
            f"error_type={session.get('error_type')} error={session.get('error')}"
        )
    trainability_annotation_count = sum(
        len(session.get("trainable_step_ids", ()))
        for session in report["sessions"]
        if session["status"] == "processed"
    )
    print(f"Trainability annotations: {trainability_annotation_count}")
    print(f"Report includes rows: {report.get('include_rows')}")
    assert report["sessions_failed"] == 0
    assert report["sessions_processed"] == len(report["sampling"]["session_ids"])
    assert report["trainable_row_count"] == trainability_annotation_count
