import click

import json

from compgraph.algorithms import pmi_graph


def run_pmi_graph(input_filepath: str, output_filepath: str) -> None:
    graph = pmi_graph(input_filepath, read_from_file=True)
    result = graph.run()
    with open(output_filepath, 'w') as out:
        json.dump(list(result), out)


@click.command()
@click.argument('input_filepath', nargs=1)
@click.argument('output_filepath', nargs=1)
def main(input_filepath: str, output_filepath: str) -> None:
    pmi_graph(input_filepath, output_filepath)


if __name__ == '__main__':
    main()
