"""Built-in best-effort job naming-convention tag stage."""

from ..stage import ETLStage, Patch, Record, StageContext


class DeriveJobTagsStage(ETLStage):
    name = "derive_job_tags"
    version = "1"
    required_fields = ("is_trainable",)
    output_fields = ("tags",)
    dependencies = ("build_chosen_trace",)

    def applies(self, record: Record, context: StageContext) -> bool:
        _ = context
        return record.get("is_trainable") is True

    def transform(self, record: Record, context: StageContext) -> Patch:
        _ = context
        try:
            parts = [part.strip() for part in str(record.get("job_id") or "").split("#")]
            if len(parts) < 4 or not all(parts[:4]):
                return {"tags": None}
            return {"tags": parts[:4]}
        except Exception:
            # Job naming is best effort and must never fail a row or batch.
            return {"tags": None}
