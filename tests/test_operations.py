import dataclasses
import typing as tp

import pytest
from pytest import approx

from compgraph import operations as ops


@dataclasses.dataclass
class MapCase:
    mapper: ops.Mapper
    data: tp.List[ops.TRow]
    ground_truth: tp.List[ops.TRow]
    cmp_keys: tp.Tuple[str, ...]


class _Key:
    def __init__(self, *args: str) -> None:
        self._items = sorted(args)

    def __call__(self, d: tp.Mapping[str, tp.Any]) -> tp.Tuple[str, ...]:
        return tuple(str(d.get(key)) for key in self._items)


MAP_CASES = [
    MapCase(
        mapper=ops.Divide(nominator='nominator', denominator='denominator', result='fraction'),
        data=[
            {'nominator': 1, 'denominator': 1},
            {'nominator': 1, 'denominator': 2},
            {'nominator': 5, 'denominator': 2},
        ],
        ground_truth=[
            {'nominator': 1, 'denominator': 1, 'fraction': approx(1, 0.001)},
            {'nominator': 1, 'denominator': 2, 'fraction': approx(0.5, 0.001)},
            {'nominator': 5, 'denominator': 2, 'fraction': approx(2.5, 0.001)},
        ],
        cmp_keys=('nominator', 'denominator', 'fraction')
    ),
    MapCase(
        mapper=ops.Log('x', 'log'),
        data=[
            {'x': 1},
            {'x': 100},
            {'x': 1e-05},
        ],
        ground_truth=[
            {'x': 1, 'log': 0},
            {'x': 100, 'log': approx(4.60517018, abs=0.001)},
            {'x': 1e-05, 'log': approx(-11.512925, abs=0.001)},
        ],
        cmp_keys=('x', 'log')
    ),
    MapCase(
        mapper=ops.WeekHour('time', '%Y/%m/%d %H%M%S.%f', 'day_of_week', 'hour'),
        data=[
            {'time': '2022/01/01 111111.000000'},
            {'time': '2010/12/04 123456.789000'},
            {'time': '2020/03/04 000000.000000'},
            {'time': '2022/12/12 000000.000000'}
        ],
        ground_truth=[
            {'time': '2022/01/01 111111.000000', 'day_of_week': 'Sat', 'hour': 11},
            {'time': '2010/12/04 123456.789000', 'day_of_week': 'Sat', 'hour': 12},
            {'time': '2020/03/04 000000.000000', 'day_of_week': 'Wed', 'hour': 0},
            {'time': '2022/12/12 000000.000000', 'day_of_week': 'Mon', 'hour': 0}
        ],
        cmp_keys=('time', 'day_of_week', 'hour')
    ),
    MapCase(
        mapper=ops.HaversineDistance('start', 'end', 'haversine_distance'),
        data=[
            {"start": [4.8422, 45.7597], "end": [2.3508, 48.8567]},  # lyon -> paris
            {"start": [15.1234, 23.123], "end": [51.123, 72.123]},
            {"start": [3.60, 77.77], "end": [23.64, 51.11]},
        ],
        ground_truth=[
            {"start": [4.8422, 45.7597], "end": [2.3508, 48.8567],
             "haversine_distance": approx(392.21671, 0.001)},
            {"start": [15.1234, 23.123], "end": [51.123, 72.123],
             "haversine_distance": approx(5890.713414, 0.001)},
            {"start": [3.60, 77.77], "end": [23.64, 51.11],
             "haversine_distance": approx(3076.82553, 1e-3)},
        ],
        cmp_keys=("start", "end", "haversine_distance")
    ),
]


@pytest.mark.parametrize("case", MAP_CASES)
def test_mapper(case: MapCase) -> None:
    key_func = _Key(*case.cmp_keys)
    result = ops.Map(case.mapper)(iter(case.data))
    assert isinstance(result, tp.Iterator)
    assert sorted(case.ground_truth, key=key_func) == sorted(result, key=key_func)
