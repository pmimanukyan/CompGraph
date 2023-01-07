import json

import click

from compgraph.algorithms import yandex_maps_graph


def run_yandex_maps_graph(input_times_filepath: str, input_length_filepath: str, output_filepath: str) -> None:
    graph = yandex_maps_graph(input_times_filepath, input_length_filepath, read_from_file=True)
    result = graph.run()
    with open(output_filepath, 'w') as out:
        json.dump(list(result), out)


@click.command()
@click.argument('input_times_filepath', nargs=1)
@click.argument('input_length_filepath', nargs=1)
@click.argument('output_filepath', nargs=1)
def main(input_times_filepath: str, input_length_filepath: str, output_filepath: str) -> None:
    run_yandex_maps_graph(input_times_filepath, input_length_filepath, output_filepath)


if __name__ == '__main__':
    main()
