"""Landing trainability stage extension point.

The landing pipeline is intentionally wired before its business rule is
available so the contributor only needs to implement this stage and its tests.
"""

from ..stage import ETLStage, Patch, Record, StageContext


class UpdateIsTrainableStage(ETLStage):
    """TODO: determine and update ``is_trainable`` for applicable landing rows."""

    name = "update_is_trainable"
    version = "1"
    required_fields = ()
    output_fields = ("is_trainable",)

    def applies(self, record: Record, context: StageContext) -> bool:
        _ = record, context
        # TODO(etl-contributor): replace this placeholder with the precise,
        # deterministic applicability rule for the trainability enrichment.
        raise NotImplementedError(
            "UpdateIsTrainableStage.applies() must be implemented before execution"
        )

    def transform(self, record: Record, context: StageContext) -> Patch:
        _ = record, context
        # TODO(etl-contributor): return only {"is_trainable": <bool>} and keep
        # the result deterministic/idempotent. The engine performs the diff and
        # update_landing() refreshes source_updated_at only for a real patch.
        raise NotImplementedError(
            "UpdateIsTrainableStage.transform() must be implemented before execution"
        )
