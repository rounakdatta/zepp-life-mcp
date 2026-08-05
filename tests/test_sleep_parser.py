from zepp_life_mcp.adapters.base import parse_sleep_summary


def test_sleep_modes_use_confirmed_mapping_and_inclusive_duration():
    metrics = parse_sleep_summary(
        {
            "stage": [
                {"mode": 4, "start": 0, "stop": 9},
                {"mode": 5, "start": 10, "stop": 19},
                {"mode": 7, "start": 20, "stop": 24},
                {"mode": 8, "start": 25, "stop": 34},
            ]
        }
    )

    assert [stage.model_dump() for stage in metrics.stages] == [
        {"stage": "light", "minutes": 10},
        {"stage": "deep", "minutes": 10},
        {"stage": "awake", "minutes": 5},
        {"stage": "rem", "minutes": 10},
    ]
    assert metrics.deep_minutes == 10
    assert metrics.light_minutes == 10
    assert metrics.rem_minutes == 10
    assert metrics.awake_minutes == 5
    assert metrics.time_asleep_minutes == 30
    assert metrics.duration_minutes == 35
    assert metrics.wake_count == 1


def test_sleep_fallback_uses_dp_lt_wk_and_never_rn_for_rem():
    metrics = parse_sleep_summary({"dp": 90, "lt": 210, "wk": 15, "rn": 45})

    assert metrics.stages == []
    assert metrics.deep_minutes == 90
    assert metrics.light_minutes == 210
    assert metrics.rem_minutes == 0
    assert metrics.awake_minutes == 15
    assert metrics.time_asleep_minutes == 300
    assert metrics.duration_minutes == 315
    assert metrics.wake_count == 0


def test_sleep_score_parsed_from_ss_with_safe_bounds():
    assert parse_sleep_summary({"ss": 82}).score == 82
    assert parse_sleep_summary({"ss": "75"}).score == 75
    assert parse_sleep_summary({}).score is None
    assert parse_sleep_summary({"ss": 255}).score is None
    assert parse_sleep_summary({"ss": -1}).score is None
    assert parse_sleep_summary({"ss": "bad"}).score is None
