from operator import itemgetter
import tempfile
import json

import click
from click.testing import CliRunner
from pytest import approx

from examples.run_word_count import run_word_count
from examples.run_inverted_index_graph import run_inverted_index_graph
from examples.run_pmi_graph import run_pmi_graph
from examples.run_yandex_maps_graph import run_yandex_maps_graph


@click.command()
@click.argument('input_file', nargs=1)
@click.argument('output_file', nargs=1)
def script_word_count(input_file: str, output_file: str) -> None:
    run_word_count(input_file, output_file)
    with open(output_file, 'r') as f:
        result = json.load(f)
    click.echo(result, nl=False)


def test_script_word_count() -> None:
    runner = CliRunner()
    input_file = tempfile.NamedTemporaryFile()
    output_file = tempfile.NamedTemporaryFile()

    docs = [
        {'doc_id': 1, 'text': 'hello, my little WORLD'},
        {'doc_id': 2, 'text': 'Hello, my little little hell'}
    ]

    expected = [
        {'text': 'hell', 'count': 1},
        {'text': 'world', 'count': 1},
        {'text': 'hello', 'count': 2},
        {'text': 'my', 'count': 2},
        {'text': 'little', 'count': 3},
    ]

    with open(input_file.name, 'w') as f:
        json.dump(docs, f)

    result = runner.invoke(script_word_count, [input_file.name, output_file.name])
    assert result.output == str(expected)


def test_script_tf_idf() -> None:
    input_file = tempfile.NamedTemporaryFile()
    output_file = tempfile.NamedTemporaryFile()

    rows = [
        {'doc_id': 1, 'text': 'hello, little world'},
        {'doc_id': 2, 'text': 'little'},
        {'doc_id': 3, 'text': 'little little little'},
        {'doc_id': 4, 'text': 'little? hello little world'},
        {'doc_id': 5, 'text': 'HELLO HELLO! WORLD...'},
        {'doc_id': 6, 'text': 'world? world... world!!! WORLD!!! HELLO!!!'}
    ]

    expected = [
        {'doc_id': 5, 'text': 'hello', 'tf_idf': approx(0.2703, 0.001)},
        {'doc_id': 1, 'text': 'hello', 'tf_idf': approx(0.1351, 0.001)},
        {'doc_id': 4, 'text': 'hello', 'tf_idf': approx(0.1013, 0.001)},
        {'doc_id': 2, 'text': 'little', 'tf_idf': approx(0.4054, 0.001)},
        {'doc_id': 3, 'text': 'little', 'tf_idf': approx(0.4054, 0.001)},
        {'doc_id': 4, 'text': 'little', 'tf_idf': approx(0.2027, 0.001)},
        {'doc_id': 6, 'text': 'world', 'tf_idf': approx(0.3243, 0.001)},
        {'doc_id': 1, 'text': 'world', 'tf_idf': approx(0.1351, 0.001)},
        {'doc_id': 5, 'text': 'world', 'tf_idf': approx(0.1351, 0.001)}
    ]

    with open(input_file.name, 'w') as f:
        json.dump(rows, f)

    run_inverted_index_graph(input_file.name, output_file.name)
    with open(output_file.name, 'r') as f:
        result = json.load(f)

    assert result == expected


def test_script_pmi_graph() -> None:
    input_file = tempfile.NamedTemporaryFile()
    output_file = tempfile.NamedTemporaryFile()

    rows = [
        {'doc_id': 1, 'text': 'hello, little world'},
        {'doc_id': 2, 'text': 'little'},
        {'doc_id': 3, 'text': 'little little little'},
        {'doc_id': 4, 'text': 'little? hello little world'},
        {'doc_id': 5, 'text': 'HELLO HELLO! WORLD...'},
        {'doc_id': 6, 'text': 'world? world... world!!! WORLD!!! HELLO!!! HELLO!!!!!!!'}
    ]

    expected = [  # Mind the order !!!
        {'doc_id': 3, 'text': 'little', 'pmi': approx(0.9555, 0.001)},
        {'doc_id': 4, 'text': 'little', 'pmi': approx(0.9555, 0.001)},
        {'doc_id': 5, 'text': 'hello', 'pmi': approx(1.1786, 0.001)},
        {'doc_id': 6, 'text': 'world', 'pmi': approx(0.7731, 0.001)},
        {'doc_id': 6, 'text': 'hello', 'pmi': approx(0.0800, 0.001)},
    ]

    with open(input_file.name, 'w') as f:
        json.dump(rows, f)

    run_pmi_graph(input_file.name, output_file.name)
    with open(output_file.name, 'r') as f:
        result = json.load(f)

    assert result == expected


def test_script_yandex_maps() -> None:
    input_times_file = tempfile.NamedTemporaryFile()
    input_length_file = tempfile.NamedTemporaryFile()
    output_file = tempfile.NamedTemporaryFile()

    lengths = [
        {'start': [37.84870228730142, 55.73853974696249], 'end': [37.8490418381989, 55.73832445777953],
         'edge_id': 8414926848168493057},
        {'start': [37.524768467992544, 55.88785375468433], 'end': [37.52415172755718, 55.88807155843824],
         'edge_id': 5342768494149337085},
        {'start': [37.56963176652789, 55.846845586784184], 'end': [37.57018438540399, 55.8469259692356],
         'edge_id': 5123042926973124604},
        {'start': [37.41463478654623, 55.654487907886505], 'end': [37.41442892700434, 55.654839486815035],
         'edge_id': 5726148664276615162},
        {'start': [37.584684155881405, 55.78285809606314], 'end': [37.58415022864938, 55.78177368734032],
         'edge_id': 451916977441439743},
        {'start': [37.736429711803794, 55.62696328852326], 'end': [37.736344216391444, 55.626937723718584],
         'edge_id': 7639557040160407543},
        {'start': [37.83196756616235, 55.76662947423756], 'end': [37.83191015012562, 55.766647034324706],
         'edge_id': 1293255682152955894},
    ]

    with open(input_length_file.name, 'w') as f:
        json.dump(lengths, f)

    times = [
        {'leave_time': '20171020T112238.723000', 'enter_time': '20171020T112237.427000',
         'edge_id': 8414926848168493057},
        {'leave_time': '20171011T145553.040000', 'enter_time': '20171011T145551.957000',
         'edge_id': 8414926848168493057},
        {'leave_time': '20171020T090548.939000', 'enter_time': '20171020T090547.463000',
         'edge_id': 8414926848168493057},
        {'leave_time': '20171024T144101.879000', 'enter_time': '20171024T144059.102000',
         'edge_id': 8414926848168493057},
        {'leave_time': '20171022T131828.330000', 'enter_time': '20171022T131820.842000',
         'edge_id': 5342768494149337085},
        {'leave_time': '20171014T134826.836000', 'enter_time': '20171014T134825.215000',
         'edge_id': 5342768494149337085},
        {'leave_time': '20171010T060609.897000', 'enter_time': '20171010T060608.344000',
         'edge_id': 5342768494149337085},
        {'leave_time': '20171027T082600.201000', 'enter_time': '20171027T082557.571000',
         'edge_id': 5342768494149337085}
    ]

    with open(input_times_file.name, 'w') as f:
        json.dump(times, f)

    run_yandex_maps_graph(input_times_file.name, input_length_file.name, output_file.name)

    expected = [
        {'weekday': 'Fri', 'hour': 8, 'speed': approx(62.2322, 0.001)},
        {'weekday': 'Fri', 'hour': 9, 'speed': approx(78.1070, 0.001)},
        {'weekday': 'Fri', 'hour': 11, 'speed': approx(88.9552, 0.001)},
        {'weekday': 'Sat', 'hour': 13, 'speed': approx(100.9690, 0.001)},
        {'weekday': 'Sun', 'hour': 13, 'speed': approx(21.8577, 0.001)},
        {'weekday': 'Tue', 'hour': 6, 'speed': approx(105.3901, 0.001)},
        {'weekday': 'Tue', 'hour': 14, 'speed': approx(41.5145, 0.001)},
        {'weekday': 'Wed', 'hour': 14, 'speed': approx(106.4505, 0.001)}
    ]

    with open(output_file.name, 'r') as f:
        result = json.load(f)

    assert sorted(result, key=itemgetter('weekday', 'hour')) == expected
