import pytest
from app.handlers import format_duration

@pytest.mark.parametrize("seconds,expected", [
    (59, "59 сек"),
    (60, "1 мин 0 сек"),
    (61, "1 мин 1 сек"),
    (3599, "59 мин 59 сек"),
    (3600, "1 ч 0 мин"),
    (3661, "1 ч 1 мин"),
    (86399, "23 ч 59 мин"),
    (86400, "1 д 0 ч"),
    (90000, "1 д 1 ч"),
    (172800, "2 д 0 ч"),
])
def test_format_duration(seconds, expected):
    assert format_duration(seconds) == expected
