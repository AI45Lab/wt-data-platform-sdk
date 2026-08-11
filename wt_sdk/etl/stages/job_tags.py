"""Built-in best-effort job naming-convention tag stage."""

from ..stage import ETLStage, Session, SessionPatch, StageContext


class DeriveJobTagsStage(ETLStage):
    name = "derive_job_tags"
    version = "1"
    required_fields = ("id", "job_id", "is_trainable")
    output_fields = ("tags",)
    job_discovery_filter = "is_trainable = true"

    def transform_session(
        self,
        session: Session,
        context: StageContext,
    ) -> SessionPatch:
        _ = context
        patches: SessionPatch = {}
        for record in session:
            if record.get("is_trainable") is not True:
                continue
            record_id = str(record["id"])
            try:
                parts = [
                    part.strip()
                    for part in str(record.get("job_id") or "").split("#")
                ]
                tags = parts[:4] if len(parts) >= 4 and all(parts[:4]) else None
            except Exception:
                # Job naming is best effort and must never fail a session.
                tags = None
            patches[record_id] = {"tags": tags}
        return patches
