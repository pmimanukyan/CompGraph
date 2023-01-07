import click

import json

from compgraph.algorithms import inverted_index_graph


def run_inverted_index_graph(input_filepath: str, output_filepath: str) -> None:
    graph = inverted_index_graph(input_filepath, read_from_file=True)
    result = graph.run()
    with open(output_filepath, 'w') as out:
        json.dump(list(result), out)


@click.command()
@click.argument('input_filepath', nargs=1)
@click.argument('output_filepath', nargs=1)
def main(input_filepath: str, output_filepath: str) -> None:
    inverted_index_graph(input_filepath, output_filepath)


if __name__ == '__main__':
    main()
