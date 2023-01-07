from collections.abc import Callable
import typing as tp
from itertools import groupby

TRow = tp.Dict[str, tp.Any]
TRowsIterable = tp.Iterable[TRow]


def keyfunc(keys: tp.Sequence[str]) -> Callable[[TRow], list[str]]:
    return lambda row: [row[i] for i in keys]


def groupby_verbose(rows: TRowsIterable, key: tp.Callable[[TRow], list[str]]) -> tp.Any:
    prev_key: tp.Any = None
    for k, group in groupby(rows, key=key):
        assert prev_key is None or prev_key <= k
        yield k, group
        prev_key = k
