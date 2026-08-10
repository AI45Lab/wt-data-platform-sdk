"""Built-in serving search-text aggregation stage."""

from ..exceptions import StageTransformError
from ..stage import ETLStage, Session, SessionPatch, StageContext


SEARCH_TEXT_SOURCE_FIELDS = (
    "chosen_trace",
    "rejected_trace",
    "agent_model",
    "meta_json",
    "dataset_type",
    "tags",
)


class BuildSearchTextStage(ETLStage):
    """Join searchable scalar/JSON strings for trainable serving rows."""

    name = "build_search_text"
    version = "2"
    required_fields = (
        "id",
        "is_trainable",
        *SEARCH_TEXT_SOURCE_FIELDS,
    )
    output_fields = ("search_text",)
    dependencies = ("build_chosen_trace", "derive_job_tags")

    def transform_session(
        self,
        session: Session,
        context: StageContext,
    ) -> SessionPatch:
        del context
        patches: SessionPatch = {}
        for record in session:
            if record.get("is_trainable") is not True:
                continue

            record_id = str(record["id"])
            parts: list[str] = []
            for field_name in SEARCH_TEXT_SOURCE_FIELDS:
                value = record.get(field_name)
                if value is None:
                    continue
                if field_name == "tags":
                    if not isinstance(value, (list, tuple)):
                        raise StageTransformError(
                            f"tags must be a list of strings or null for record "
                            f"{record_id!r}",
                            record_id=record_id,
                        )
                    for tag in value:
                        if tag is None:
                            continue
                        if not isinstance(tag, str):
                            raise StageTransformError(
                                f"tags must contain only strings or null for record "
                                f"{record_id!r}",
                                record_id=record_id,
                            )
                        if tag.strip():
                            parts.append(tag)
                    continue
                if not isinstance(value, str):
                    raise StageTransformError(
                        f"{field_name} must be a string or null for record "
                        f"{record_id!r}",
                        record_id=record_id,
                    )
                if value.strip():
                    parts.append(value)

            patches[record_id] = {
                "search_text": "\n".join(parts) if parts else None,
            }
        return patches


__all__ = ["BuildSearchTextStage", "SEARCH_TEXT_SOURCE_FIELDS"]
