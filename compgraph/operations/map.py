import string
import typing as tp
import re
import math
from datetime import datetime
from copy import deepcopy

from .abstract import Operation, Mapper


TRow = tp.Dict[str, tp.Any]
TRowsIterable = tp.Iterable[TRow]
TRowsGenerator = tp.Generator[TRow, None, None]


class Map(Operation):
    def __init__(self, mapper: Mapper) -> None:
        self.mapper = mapper

    def __call__(self, rows: TRowsIterable, *args: tp.Any,
                 **kwargs: tp.Any) -> TRowsGenerator:
        for row in rows:
            yield from self.mapper(row)


# Dummy mapper
class DummyMapper(Mapper):
    """Yield exactly the row passed"""

    def __call__(self, row: TRow) -> TRowsGenerator:
        yield row


# Mappers
class FilterPunctuation(Mapper):
    """Left only non-punctuation symbols"""

    def __init__(self, column: str):
        """
        :param column: name of column to process
        """
        self.column = column
        self.table = str.maketrans('', '', string.punctuation)

    def __call__(self, row: TRow) -> TRowsGenerator:
        row[self.column] = row[self.column].translate(self.table)
        yield row


class Filter(Mapper):
    """Remove records that don't satisfy some condition"""

    def __init__(self, condition: tp.Callable[[TRow], bool]) -> None:
        """
        :param condition: if condition is not true - remove record
        """
        self.condition = condition

    def __call__(self, row: TRow) -> TRowsGenerator:
        if self.condition(row):
            yield row


class Project(Mapper):
    """Leave only mentioned columns"""

    def __init__(self, columns: tp.Sequence[str]) -> None:
        """
        :param columns: names of columns
        """
        self.columns = columns

    def __call__(self, row: TRow) -> TRowsGenerator:
        yield {col: row[col] for col in self.columns}


class LowerCase(Mapper):
    """Replace column value with value in lower case"""

    def __init__(self, column: str):
        """
        :param column: name of column to process
        """
        self.column = column

    @staticmethod
    def _lower_case(txt: str) -> str:
        return txt.lower()

    def __call__(self, row: TRow) -> TRowsGenerator:
        row[self.column] = self._lower_case(row[self.column])
        yield row


class Split(Mapper):
    """Splits row on multiple rows by separator"""

    def __init__(self, column: str,
                 separator: str | None = None) -> None:
        """
        :param column: name of column to split
        :param separator: string to separate by
        """
        self.column = column
        self.separator = separator if separator is not None else r'\s+'

    def __call__(self, row: TRow) -> TRowsGenerator:
        start = 0
        original_row = row.copy()
        for match in re.finditer(self.separator, original_row[self.column]):
            row = original_row.copy()
            row[self.column] = row[self.column][start:match.start()]
            start = match.end()
            yield row

        row = original_row.copy()
        row[self.column] = row[self.column][start:]
        yield row


class Product(Mapper):
    """Calculates product of multiple columns"""

    def __init__(self, columns: tp.Sequence[str],
                 result_column: str = 'product') -> None:
        """
        :param columns: column names to product
        :param result_column: column name to save product in
        """
        self.columns = columns
        self.result_column = result_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        prod = 1
        for col in self.columns:
            prod *= row[col]
        row_copy = deepcopy(row)
        row_copy[self.result_column] = prod
        yield row_copy


class Divide(Mapper):
    """
    Divide one column by another
    """

    def __init__(self, nominator: str, denominator: str, result: str) -> None:
        """
        :param result: result column name
        """
        self.nominator = nominator
        self.denominator = denominator
        self.result = result

    def __call__(self, row: TRow) -> TRowsGenerator:
        assert row[self.denominator] != 0
        row[self.result] = row[self.nominator] / row[self.denominator]
        yield row


class Log(Mapper):
    """
    Get logarithm of a column
    """

    def __init__(self, arg: str, result: str) -> None:
        """
        :param arg: column to log
        :param result: result column
        """
        self.arg = arg
        self.result = result

    def __call__(self, row: TRow) -> TRowsGenerator:
        row[self.result] = math.log(row[self.arg])
        yield row


class WeekHour(Mapper):
    """
    Extract weekday and hour from time using given time format to parse
    """

    def __init__(self, time: str, time_format: str, weekday_result: str,
                 hour_result: str) -> None:
        """
        :param time: time to parse
        :param format:
        :param weekday_result: result columns for weekday
        :param hour_result: result columns for hour
        """
        self.time = time
        self.time_format = time_format
        self.weekday_result = weekday_result
        self.hour_result = hour_result

    def __call__(self, row: TRow) -> TRowsGenerator:
        try:
            dt = datetime.strptime(row[self.time], self.time_format)
        except ValueError:
            dt = datetime.strptime(row[self.time], '%Y%m%dT%H%M%S')

        row[self.weekday_result] = dt.strftime("%A")[:3]
        row[self.hour_result] = dt.hour
        yield row


class HaversineDistance(Mapper):
    """
    Find distance between two points
    """
    EARTH_RADIUS_KM = 6371.0

    def __init__(self, start: str, end: str, result: str) -> None:
        """
        :param start: column name of [lon, lat] of start point
        :param end: column name of [lon, lat] of end point
        :param result: result column name
        """
        self.start = start
        self.end = end
        self.result = result

    def __call__(self, row: TRow) -> TRowsGenerator:
        if self.result in row:
            yield row
            return

        lng1, lat1 = row[self.start]
        lng2, lat2 = row[self.end]

        lng1, lat1, lng2, lat2 = map(math.radians, [lng1, lat1, lng2, lat2])

        dlng = lng2 - lng1
        dlat = lat2 - lat1

        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlng/2)**2
        c = 2 * math.asin(math.sqrt(a))

        row[self.result] = c * self.EARTH_RADIUS_KM
        yield row
