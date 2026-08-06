"""Landing trainability stage extension point.

The landing pipeline is intentionally wired before its business rule is
available so the contributor only needs to implement this stage and its tests.
"""

from ..stage import ETLStage, Session, SessionPatch, StageContext


class UpdateIsTrainableStage(ETLStage):
    """TODO: determine and update ``is_trainable`` for applicable landing rows."""

    name = "update_is_trainable"
    version = "1"
    required_fields = ()
    output_fields = ("is_trainable",)

    def transform_session(
        self,
        session: Session,
        context: StageContext,
    ) -> SessionPatch:
        _ = session, context
        # TODO(etl-contributor): analyze the complete immutable session and
        # return {record_id: {"is_trainable": <bool>}} for the rows whose
        # desired value this stage owns. The engine validates and merges the
        # result, then refreshes source_updated_at only for a real final diff.
        raise NotImplementedError(
            "UpdateIsTrainableStage.transform_session() must be implemented "
            "before execution"
        )
