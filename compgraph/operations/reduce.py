import typing as tp
import copy
import heapq
from datetime import datetime
from collections import Counter

from .abstract import Operation, Reducer
from .utils import keyfunc, groupby_verbose

TRow = tp.Dict[str, tp.Any]
TRowsIterable = tp.Iterable[TRow]
TRowsGenerator = tp.Generator[TRow, None, None]


class Reduce(Operation):
    def __init__(self, reducer: Reducer, keys: tp.Sequence[str]) -> None:
        self.reducer = reducer
        self.keys = keys

    def __call__(self, rows: TRowsIterable, *args: tp.Any,
                 **kwargs: tp.Any) -> TRowsGenerator:
        if not self.keys:
            yield from self.reducer((), rows)
            return

        for key, group in groupby_verbose(rows, key=keyfunc(self.keys)):
            yield from self.reducer(tuple(self.keys), group)


# Dummy reducer
class FirstReducer(Reducer):
    """Yield only first row from passed ones"""

    def __call__(self, group_key: tp.Tuple[str, ...],
                 rows: TRowsIterable) -> TRowsGenerator:
        for row in rows:
            yield row
            break


# Reducers
class Count(Reducer):
    """
    Counts records by key
    Example for group_key=('a',) and column='d'
        {'a': 1, 'b': 5, 'c': 2}
        {'a': 1, 'b': 6, 'c': 1}
        =>
        {'a': 1, 'd': 2}
    """

    def __init__(self, column: str) -> None:
        """
        :param column: name for result column
        """
        self.column = column

    def __call__(self, group_key: tp.Tuple[str, ...],
                 rows: TRowsIterable) -> TRowsGenerator:
        cnt = 0
        res_row: TRow = {}
        for row in rows:
            if not res_row:
                res_row = {col: row[col] for col in group_key}
            cnt += 1

        res_row[self.column] = cnt
        yield res_row


class CountUnique(Reducer):
    """Count the number of unique values of specific column"""

    def __init__(self, column: str, result_column: str) -> None:
        """
        :param column: name of column from where to count unique values
        :param result_column: result column name
        """
        self.column = column
        self.result_column = result_column

    def __call__(self, group_key: tp.Tuple[str, ...],
                 rows: TRowsIterable) -> TRowsGenerator:
        values_set = set()
        res_row: TRow = {}
        for row in rows:
            if not res_row and group_key:
                res_row = {col: row[col] for col in group_key}
            if row[self.column] not in values_set:
                values_set.add(row[self.column])
        res_row[self.result_column] = len(values_set)
        yield res_row


class TopN(Reducer):
    """Calculate top N by value"""

    def __init__(self, column: str, n: int) -> None:
        """
        :param column: column name to get top by
        :param n: number of top values to extract
        """
        self.column_max = column
        self.n = n

    def __call__(self, group_key: tp.Tuple[str, ...],
                 rows: TRowsIterable) -> TRowsGenerator:
        yield from heapq.nlargest(self.n, rows, key=lambda row: row[self.column_max])


class TermFrequency(Reducer):
    """Calculate frequency of values in column"""

    def __init__(self, words_column: str, result_column: str = 'tf') -> None:
        """
        :param words_column: name for column with words
        :param result_column: name for result column
        """
        self.words_column = words_column
        self.result_column = result_column

    def __call__(self, group_key: tp.Tuple[str, ...],
                 rows: TRowsIterable) -> TRowsGenerator:
        cnt_words = 0
        counter: dict[tp.Any, int] = Counter()
        res_row: TRow = {}

        for row in rows:
            if not res_row:
                res_row = {col: row[col] for col in group_key}
            if row[self.words_column] not in counter:
                counter[row[self.words_column]] = 1
            else:
                counter[row[self.words_column]] += 1
            cnt_words += 1

        for key, val in counter.items():
            tf = val / cnt_words
            row = copy.copy(res_row)
            row[self.words_column] = key
            row[self.result_column] = tf
            yield row


class Sum(Reducer):
    """
    Sum values aggregated by key
    Example for key=('a',) and column='b'
        {'a': 1, 'b': 2, 'c': 4}
        {'a': 1, 'b': 3, 'c': 5}
        =>
        {'a': 1, 'b': 5}
    """

    def __init__(self, column: str) -> None:
        """
        :param column: name for sum column
        """
        self.column = column

    def __call__(self, group_key: tp.Tuple[str, ...],
                 rows: TRowsIterable) -> TRowsGenerator:
        sum_ = 0
        res_row: TRow = {}
        for row in rows:
            if not res_row:
                res_row = {col: row[col] for col in group_key}
            sum_ += row[self.column]
        res_row[self.column] = sum_
        yield res_row


class Speed(Reducer):
    """
    Calculate avarage speed for specific weekday and hour
    """
    A_MICROSECOND_IN_SECONDS = 10**(-6)
    AN_HOUR_IN_SECONDS = 3600

    def __init__(self, distance: str, start: str, end: str, time_format: str,
                 result: str) -> None:
        """
        :param distance: distance
        :param start: start time
        :param end: end time
        :param time_format: time format
        :param result: result column name
        """
        self.start = start
        self.end = end
        self.distance = distance
        self.time_format = time_format
        self.result = result

    def __call__(self, group_key: tp.Tuple[str, ...],
                 rows: TRowsIterable) -> TRowsGenerator:
        all_distance: float = 0
        all_time: float = 0
        res_row: TRow = {}

        def get_dt(str_to_datetime: str) -> datetime:
            try:
                dt = datetime.strptime(str_to_datetime, self.time_format)
            except ValueError:
                dt = datetime.strptime(str_to_datetime, '%Y%m%dT%H%M%S')
            return dt

        for row in rows:
            if not res_row:
                res_row = {col: row[col] for col in group_key}
            dt_1 = get_dt(row[self.start])
            dt_2 = get_dt(row[self.end])

            td = dt_2 - dt_1
            all_time += (td.seconds + td.microseconds * self.A_MICROSECOND_IN_SECONDS) / self.AN_HOUR_IN_SECONDS
            all_distance += row[self.distance]

        res_row[self.result] = all_distance / all_time
        yield res_row
