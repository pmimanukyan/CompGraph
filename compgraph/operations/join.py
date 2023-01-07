import typing as tp

from .abstract import Operation, Joiner
from .utils import keyfunc, groupby_verbose

TRow = tp.Dict[str, tp.Any]
TRowsIterable = tp.Iterable[TRow]
TRowsGenerator = tp.Generator[TRow, None, None]


class Join(Operation):
    def __init__(self, joiner: Joiner, keys: tp.Sequence[str]):
        self.keys = keys
        self.joiner = joiner

    def __call__(self, rows: TRowsIterable, *columns: tp.Any,
                 **kwcolumns: tp.Any) -> TRowsGenerator:
        left = groupby_verbose(rows, key=keyfunc(self.keys))
        right = groupby_verbose(columns[0], key=keyfunc(self.keys))

        l_key, l_rows = next(left, (None, None))
        r_key, r_rows = next(right, (None, None))
        while (l_key or r_key) is not None:
            if r_key is None or (l_key is not None and l_key < r_key):
                if l_rows is not None:
                    yield from self.joiner(self.keys, l_rows, [])
                l_key, l_rows = next(left, (None, None))
            elif l_key is None or (r_key is not None and r_key < l_key):
                if r_rows is not None:
                    yield from self.joiner(self.keys, [], r_rows)
                r_key, r_rows = next(right, (None, None))
            else:
                if l_rows is not None and r_rows is not None:
                    yield from self.joiner(self.keys, l_rows, r_rows)
                l_key, l_rows = next(left, (None, None))
                r_key, r_rows = next(right, (None, None))


# Joiners
def merge_two_rows(keys: tp.Sequence[str], row_a: TRow,
                   row_b: TRow, a_suffix: str, b_suffix: str) -> TRow:
    ans = {key: row_a[key] for key in keys}
    for i in row_a:
        if i not in keys:
            if i in row_b:
                ans[i + a_suffix] = row_a[i]
            else:
                ans[i] = row_a[i]
    for i in row_b:
        if i not in keys:
            if i in row_a:
                ans[i + b_suffix] = row_b[i]
            else:
                ans[i] = row_b[i]
    return ans


class InnerJoiner(Joiner):
    """Join with inner strategy"""

    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable,
                 rows_b: TRowsIterable) -> TRowsGenerator:
        rows_b = [row for row in rows_b]
        for row_a in rows_a:
            for row_b in rows_b:
                yield merge_two_rows(keys, row_a, row_b, self._a_suffix,
                                     self._b_suffix)


class OuterJoiner(Joiner):
    """Join with outer strategy"""

    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable,
                 rows_b: TRowsIterable) -> TRowsGenerator:
        rows_a = [row for row in rows_a]
        rows_b = [row for row in rows_b]
        if len(rows_a) == 0:
            yield from rows_b
        elif len(rows_b) == 0:
            yield from rows_a
        else:
            for row_a in rows_a:
                for row_b in rows_b:
                    yield merge_two_rows(keys, row_a, row_b, self._a_suffix,
                                         self._b_suffix)


class LeftJoiner(Joiner):
    """Join with left strategy"""

    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable,
                 rows_b: TRowsIterable) -> TRowsGenerator:
        rows_b = [row for row in rows_b]
        if len(rows_b) == 0:
            yield from rows_a
        else:
            for row_a in rows_a:
                for row_b in rows_b:
                    yield merge_two_rows(keys, row_a, row_b, self._a_suffix,
                                         self._b_suffix)


class RightJoiner(Joiner):
    """Join with right strategy"""

    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable,
                 rows_b: TRowsIterable) -> TRowsGenerator:
        rows_a = [row for row in rows_a]
        if len(rows_a) == 0:
            yield from rows_b
        else:
            for row_b in rows_b:
                for row_a in rows_a:
                    yield merge_two_rows(keys, row_a, row_b, self._a_suffix,
                                         self._b_suffix)
