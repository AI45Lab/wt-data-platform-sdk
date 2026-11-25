import pytest

from wt_sdk.trajectory_stats.categories import DeliveryCategory, parse_delivery_category


@pytest.mark.parametrize("raw_harness", ["claude", "claude-code", "claude_code", "claudecode"])
def test_harness_aliases_and_dataset_are_normalized(raw_harness):
    category = parse_delivery_category(
        f" CyberGym # {raw_harness} # glm-5.3 # 挖掘 # level1-part2 "
    )

    assert category == DeliveryCategory(
        dataset="cybergym",
        harness="claudecode",
        model="glm-5.3",
        task_type="挖掘",
        level="level1-part2",
    )
    assert category.key == ("cybergym", "claudecode", "glm-5.3", "挖掘", "level1-part2")


def test_cybergym_uses_first_nonempty_level_segment_after_task_type():
    category = parse_delivery_category(
        "cybergym#codex#Model A#利用#other-info#level#level2-part1#level3"
    )

    assert category.level == "level2-part1"
    assert category.model == "Model A"


def test_non_cybergym_never_uses_level_segments():
    category = parse_delivery_category("cvefactory#codex#Model#修复#level9")

    assert category == DeliveryCategory("cvefactory", "codex", "Model", "修复", "")


@pytest.mark.parametrize(
    "job_id",
    [None, "", "one#two#three", "one##model#task", "one#harness##task", "#"],
)
def test_invalid_job_ids_do_not_form_a_category(job_id):
    assert parse_delivery_category(job_id) is None
