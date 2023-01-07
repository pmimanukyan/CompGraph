import typing as tp

from . import operations as ops
from . import external_sort


class Graph:
    """Computational graph implementation"""

    def __init__(self) -> None:
        self.__operation: ops.Operation | None = None
        self._parents: list[Graph] = []

    @staticmethod
    def _graph_maker(operation: ops.Operation, graphs: list['Graph'] = []) -> 'Graph':
        graph = Graph()
        graph.__operation = operation
        graph._parents = graphs
        return graph

    @staticmethod
    def graph_from_iter(name: str) -> 'Graph':
        """Construct new graph which reads data from row iterator (in form of sequence of Rows
        from 'kwargs' passed to 'run' method) into graph data-flow
        Use ops.ReadIterFactory
        :param name: name of kwarg to use as data source
        """
        return Graph._graph_maker(ops.ReadIterFactory(name))

    @staticmethod
    def graph_from_file(filename: str,
                        parser: tp.Callable[[str], ops.TRow]) -> 'Graph':
        """Construct new graph extended with operation for reading rows from file
        Use ops.Read
        :param filename: filename to read from
        :param parser: parser from string to Row
        """
        return Graph._graph_maker(ops.Read(filename, parser))

    def map(self, mapper: ops.Mapper) -> 'Graph':
        """Construct new graph extended with map operation with particular mapper
        :param mapper: mapper to use
        """
        return Graph._graph_maker(ops.Map(mapper), [self])

    def reduce(self, reducer: ops.Reducer, keys: tp.Sequence[str]) -> 'Graph':
        """Construct new graph extended with reduce operation with particular reducer
        :param reducer: reducer to use
        :param keys: keys for grouping
        """
        return Graph._graph_maker(ops.Reduce(reducer, keys), [self])

    def sort(self, keys: tp.Sequence[str]) -> 'Graph':
        """Construct new graph extended with sort operation
        :param keys: sorting keys (typical is tuple of strings)
        """
        return Graph._graph_maker(external_sort.ExternalSort(keys), [self])

    def join(self, joiner: ops.Joiner, join_graph: 'Graph',
             keys: tp.Sequence[str]) -> 'Graph':
        """Construct new graph extended with join operation with another graph
        :param joiner: join strategy to use
        :param join_graph: other graph to join with
        :param keys: keys for grouping
        """
        return Graph._graph_maker(ops.Join(joiner, keys), [self, join_graph])

    def run(self, **kwargs: tp.Any) -> ops.TRowsIterable:
        """Single method to start execution; data sources passed as kwargs"""
        if self.__operation is not None:
            if len(self._parents) == 2:
                yield from self.__operation(self._parents[0].run(**kwargs), self._parents[1].run(**kwargs))
            elif len(self._parents) == 1:
                yield from self.__operation(self._parents[0].run(**kwargs))
            else:
                yield from self.__operation(**kwargs)
