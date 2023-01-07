from compgraph import operations as ops
from compgraph.graph import Graph


def test_graph_map() -> None:
    data = [
        {'text': "hello", 'x': 1},
        {'text': "world", 'x': -1}
    ]
    expected = data
    graph = Graph().graph_from_iter('input').map(ops.DummyMapper())
    assert expected == list(graph.run(input=lambda: iter(data)))


def test_graph_reduce() -> None:
    data = [
        {'first_name': 'Alex', 'transaction_id': 1001},
        {'first_name': 'Alex', 'transaction_id': 1002},
        {'first_name': 'David', 'transaction_id': 1003},
        {'first_name': 'David', 'transaction_id': 1004},
        {'first_name': 'David', 'transaction_id': 1005}
    ]
    expected = [
        {'first_name': 'Alex', 'transaction_id': 1001},
        {'first_name': 'David', 'transaction_id': 1003}
    ]
    graph = Graph().graph_from_iter('input').reduce(ops.FirstReducer(), ['first_name'])
    assert expected == list(graph.run(input=lambda: iter(data)))


def test_graph_join() -> None:
    Customers = [
        {'customer_id': 1, 'first_name': 'Alex'},
        {'customer_id': 3, 'first_name': 'David'},
        {'customer_id': 11111, 'first_name': 'Donald'}
    ]
    Orders = [
        {'order_id': 1, 'customer_id': 1, 'amount': 1000},
        {'order_id': 2, 'customer_id': 3, 'amount': 2000},
        {'order_id': 3, 'customer_id': 5, 'amount': 10000}
    ]
    expected = [
        {'order_id': 1, 'customer_id': 1, 'first_name': 'Alex', 'amount': 1000},
        {'order_id': 2, 'customer_id': 3, 'first_name': 'David', 'amount': 2000}
    ]

    graph_1 = Graph().graph_from_iter('first_input')
    graph_2 = Graph().graph_from_iter('second_input')
    graph = graph_1.join(ops.InnerJoiner(), graph_2, ['customer_id'])
    assert expected == list(graph.run(first_input=lambda: iter(Customers), second_input=lambda: iter(Orders)))


def test_graph_sort() -> None:
    data = [
        {'first_name': 'John', 'amount': 300},
        {'first_name': 'Donald', 'amount': 500},
        {'first_name': 'Alex', 'amount': 100},
    ]
    expected = [
        {'first_name': 'Alex', 'amount': 100},
        {'first_name': 'John', 'amount': 300},
        {'first_name': 'Donald', 'amount': 500},
    ]

    graph = Graph().graph_from_iter('input').sort(['amount'])
    assert expected == list(graph.run(input=lambda: iter(data)))
